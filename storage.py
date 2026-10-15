"""Bounded PostgreSQL snapshots and transactional durable outbox."""
import asyncio
import json
import time

from engine import Candle, _decimal, from_dict
from decimal import Decimal
from policy import SEED_META_KEY, STATUSES, validate_seed

SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_symbols (
    symbol TEXT PRIMARY KEY, state JSONB NOT NULL, updated_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS bot_candles (symbol TEXT PRIMARY KEY, candles JSONB NOT NULL);
CREATE TABLE IF NOT EXISTS bot_meta (key TEXT PRIMARY KEY, value JSONB NOT NULL);
CREATE TABLE IF NOT EXISTS bot_pending (
    id TEXT PRIMARY KEY, symbol TEXT NOT NULL, body TEXT NOT NULL,
    detected_ms BIGINT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
    due_ms BIGINT NOT NULL DEFAULT 0
);
ALTER TABLE bot_pending ADD COLUMN IF NOT EXISTS reply_markup JSONB;
CREATE TABLE IF NOT EXISTS bot_watch (
    symbol TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('APPROVED','BLOCKED','REVIEW','PENDING')),
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    listing_key TEXT NOT NULL DEFAULT '', notice_id TEXT NOT NULL DEFAULT '',
    message_id BIGINT, updated_ms BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS bot_watch_notice ON bot_watch(notice_id);
CREATE INDEX IF NOT EXISTS bot_pending_due ON bot_pending(due_ms, detected_ms);
"""


class Backpressure(RuntimeError):
    pass


class PolicyConflict(RuntimeError):
    """A concurrent or already processed approval must never be overwritten."""
    pass


def _decode(value):
    return json.loads(value) if isinstance(value, str) else value


class Store:
    def __init__(self, url, max_pending=2000, connect=None):
        self.url = url
        self.max_pending = max_pending
        self.connect_fn = connect
        self.conn = None
        self.lock = asyncio.Lock()
        self.history_revisions = {}

    async def open(self):
        if self.connect_fn is None:
            import asyncpg
            self.connect_fn = asyncpg.connect
        conn = await self.connect_fn(self.url, timeout=15, command_timeout=20)
        try:
            # Session lock: never run two bots against this database concurrently.
            if not await conn.fetchval('SELECT pg_try_advisory_lock(7319052026)'):
                raise RuntimeError('Another bot owns the database lock')
            await conn.execute(SCHEMA)
        except BaseException:
            await conn.close()
            raise
        self.conn = conn

    async def close(self):
        async with self.lock:
            if self.conn:
                await self.conn.close()
                self.conn = None

    async def seed_policy(self, config):
        """Insert the validated baseline once, preserving every subsequent choice."""
        config = validate_seed(config)
        stamp = int(time.time() * 1000)
        marker = {key: config[key] for key in ('version', 'sha256', 'snapshot_ms', 'source_sha256')}
        async with self.lock:
            async with self.conn.transaction():
                current = await self.conn.fetchval('SELECT value FROM bot_meta WHERE key=$1 FOR UPDATE', SEED_META_KEY)
                if current is not None:
                    if _decode(current) != marker:
                        raise ValueError('Approval seed differs from the durable baseline; refusing to overwrite')
                    rows = await self.conn.fetch('SELECT symbol,status,generation FROM bot_watch')
                    by_symbol = {row['symbol']: row for row in rows}
                    if (not set(config['snapshot']) <= by_symbol.keys()
                            or any(row['status'] not in STATUSES or row['generation'] < 1 for row in rows)):
                        raise ValueError('Incomplete or invalid durable approval policy')
                    return False
                if await self.conn.fetchval('SELECT count(*) FROM bot_watch'):
                    raise ValueError('Approval rows have no trusted seed marker; manual verification required')
                for key, status in (('approved', 'APPROVED'), ('blocked', 'BLOCKED'), ('review', 'REVIEW')):
                    await self.conn.executemany(
                        'INSERT INTO bot_watch(symbol,status,generation,listing_key,notice_id,updated_ms) '
                        'VALUES($1,$2,1,\'\',\'\',$3)',
                        [(symbol, status, stamp) for symbol in config[key]])
                await self.conn.execute(
                    'INSERT INTO bot_meta VALUES($1,$2::jsonb)', SEED_META_KEY, json.dumps(marker))
        return True

    async def load_policy(self):
        async with self.lock:
            rows = await self.conn.fetch('SELECT * FROM bot_watch')
        return {row['symbol']: dict(row) for row in rows}

    async def load_states(self, approved_symbols):
        """Stream states, selecting candle JSON only for approved contracts in SQL.

        Nonapproved A/B flags remain available for restart-safe reapproval. Their
        historic candle rows stay in PostgreSQL and are never decoded or copied.
        """
        states = {}
        revisions = {}
        async with self.lock:
            async with self.conn.transaction():
                query = ('SELECT s.symbol,s.state,c.candles FROM bot_symbols s '
                         'LEFT JOIN bot_candles c ON c.symbol=s.symbol '
                         'AND s.symbol = ANY($1::text[])')
                async for row in self.conn.cursor(query, list(approved_symbols), prefetch=16):
                    data = dict(_decode(row['state']))
                    # One JSON row at a time, not a second full-universe snapshot.
                    data['candles'] = _decode(row['candles']) if row['candles'] else []
                    state = from_dict(data)
                    states[row['symbol']] = state
                    revisions[row['symbol']] = state.history_revision
                meta = {row['key']: _decode(row['value'])
                        for row in await self.conn.fetch('SELECT key,value FROM bot_meta')}
        self.history_revisions = revisions
        return states, meta

    async def load_history(self, symbol):
        async with self.lock:
            value = await self.conn.fetchval('SELECT candles FROM bot_candles WHERE symbol=$1', symbol)
        return [Candle(int(item['open_ms']), int(item['close_ms']),
                       _decimal(item['close']) or Decimal('NaN')) for item in (_decode(value) or [])]

    async def load(self):
        """Legacy compatibility only; v4 startup uses selective load_states()."""
        async with self.lock:
            rows = await self.conn.fetch('SELECT s.symbol,s.state,c.candles FROM bot_symbols s LEFT JOIN bot_candles c USING(symbol)')
            meta = await self.conn.fetch('SELECT key,value FROM bot_meta')
        states = {r['symbol']: {**_decode(r['state']), 'candles': _decode(r['candles']) if r['candles'] else []} for r in rows}
        self.history_revisions = {symbol: value.get('history_revision', 0) for symbol, value in states.items()}
        return states, {r['key']: _decode(r['value']) for r in meta}

    async def count_pending(self):
        async with self.lock:
            return await self.conn.fetchval('SELECT count(*) FROM bot_pending')

    async def commit(self, states=(), events=(), meta=None, delete=(), watch=()):
        """Persist event flags and pending messages in the SAME transaction."""
        stamp = int(time.time() * 1000)
        states, events, watch = tuple(states), tuple(events), tuple(watch)
        changed_histories = {}
        async with self.lock:
            async with self.conn.transaction():
                for record in watch:
                    await self._write_watch(record, stamp)
                count = await self.conn.fetchval('SELECT count(*) FROM bot_pending')
                if count + len(events) > self.max_pending:
                    raise Backpressure('Outbox full; new detection paused')
                for symbol, payload in states:
                    payload = dict(payload)
                    has_candles = 'candles' in payload
                    candles = payload.pop('candles', None)
                    if has_candles and self.history_revisions.get(symbol) != payload.get('history_revision', 0):
                        await self.conn.execute('INSERT INTO bot_candles VALUES($1,$2::jsonb) ON CONFLICT(symbol) DO UPDATE SET candles=EXCLUDED.candles', symbol, json.dumps(candles))
                        changed_histories[symbol] = payload.get('history_revision', 0)
                    await self.conn.execute(
                        'INSERT INTO bot_symbols VALUES($1,$2::jsonb,$3) '
                        'ON CONFLICT(symbol) DO UPDATE SET state=EXCLUDED.state, '
                        'updated_at=EXCLUDED.updated_at', symbol, json.dumps(payload), stamp)
                for event in events:
                    await self.conn.execute(
                        'INSERT INTO bot_pending(id,symbol,body,detected_ms,reply_markup) VALUES($1,$2,$3,$4,$5::jsonb) '
                        'ON CONFLICT(id) DO NOTHING', event.id, event.symbol,
                        event.message, event.detected_ms, json.dumps(getattr(event, 'reply_markup', None)))
                for key, value in (meta or {}).items():
                    await self.conn.execute(
                        'INSERT INTO bot_meta VALUES($1,$2::jsonb) ON CONFLICT(key) '
                        'DO UPDATE SET value=EXCLUDED.value', key, json.dumps(value))
                for symbol in delete:
                    # Pending detected signals deliberately survive delisting.
                    await self.conn.execute('DELETE FROM bot_symbols WHERE symbol=$1', symbol)
                    await self.conn.execute('DELETE FROM bot_candles WHERE symbol=$1', symbol)
            self.history_revisions.update(changed_histories)
            for symbol in delete:
                self.history_revisions.pop(symbol, None)

    async def first_pending(self, now_ms):
        async with self.lock:
            row = await self.conn.fetchrow(
                'SELECT * FROM bot_pending WHERE due_ms <= $1 '
                'ORDER BY detected_ms,id LIMIT 1', now_ms)
        if row is None:
            return None
        result = dict(row)
        if result.get('reply_markup') is not None:
            result['reply_markup'] = _decode(result['reply_markup'])
        return result

    async def complete(self, event_id, message_id=None):
        async with self.lock:
            async with self.conn.transaction():
                if type(message_id) is int and message_id > 0:
                    await self.conn.execute('UPDATE bot_watch SET message_id=$2 WHERE notice_id=$1', event_id, message_id)
                await self.conn.execute('DELETE FROM bot_pending WHERE id=$1', event_id)

    async def _write_watch(self, record, stamp):
        symbol, status = record['symbol'], record['status']
        generation = record['generation']
        if (status not in STATUSES or type(generation) is not int or generation < 1
                or not isinstance(symbol, str) or not symbol.endswith('USDT')):
            raise ValueError('Invalid watch policy row')
        current = await self.conn.fetchrow('SELECT generation,status,notice_id,message_id FROM bot_watch WHERE symbol=$1 FOR UPDATE', symbol)
        expected = record.get('expected_generation')
        if current is None:
            if expected is not None:
                raise PolicyConflict('Approval contract no longer exists')
            await self.conn.execute(
                'INSERT INTO bot_watch(symbol,status,generation,listing_key,notice_id,message_id,updated_ms) '
                'VALUES($1,$2,$3,$4,$5,$6,$7)', symbol, status, generation,
                record.get('listing_key', ''), record.get('notice_id', ''), record.get('message_id'), record.get('updated_ms', stamp))
            return
        if (type(expected) is not int or current['generation'] != expected
                or (record.get('expected_status') is not None and current['status'] != record['expected_status'])
                or generation != expected + 1):
            raise PolicyConflict('Approval generation has already changed')
        message_id = record.get('message_id')
        if message_id is None and current.get('notice_id', '') == record.get('notice_id', ''):
            # A sender acknowledgement can reach PostgreSQL before the Bot cache.
            # Preserve that durable reference for this request; a new notice resets it.
            message_id = current.get('message_id')
        await self.conn.execute(
            'UPDATE bot_watch SET status=$2,generation=$3,listing_key=$4,notice_id=$5,message_id=$6,updated_ms=$7 WHERE symbol=$1',
            symbol, status, generation, record.get('listing_key', ''), record.get('notice_id', ''),
            message_id, record.get('updated_ms', stamp))

    async def retry(self, event_id, attempts, due_ms):
        async with self.lock:
            await self.conn.execute(
                'UPDATE bot_pending SET attempts=$2,due_ms=$3 WHERE id=$1',
                event_id, attempts, due_ms)

    async def retire_legacy_diagnostics(self):
        """Retire removed filters only; leave signal records and histories intact."""
        def decode(value):
            return json.loads(value) if isinstance(value, str) else value

        def obsolete(key):
            parts = str(key).upper().split(':')
            # Service error details may mention removed services; only its name counts.
            kind = parts[1] if parts[0] == 'SERVICE' and len(parts) > 1 else parts[-1]
            kind = kind.split(' ', 1)[0]
            return any(kind == prefix or kind.startswith(prefix + '_')
                       for prefix in ('COINGECKO', 'CLASSIFICATION', 'FDV'))

        def cleaned_body(body):
            lines = body.splitlines()
            kept = [line for line in lines if not obsolete(line.split(': ', 1)[0].strip())]
            if kept == lines:
                return body
            # A summary's old totals include the retired types and cannot stay accurate.
            kept = [line for line in kept if not line.startswith('새 장애 유형 ')]
            content = [line for line in kept if line.strip() and not line.startswith((
                '진단 요약', '종목별 상세:', '모든 상세 유형과 종목은 '))]
            return '\n'.join(kept) if content else None

        obsolete_meta = {'cg_budget', 'details', 'coin_candidates', 'coin_list_ms'}
        async with self.lock:
            async with self.conn.transaction():
                for row in await self.conn.fetch('SELECT key,value FROM bot_meta'):
                    key = row['key']
                    if key in obsolete_meta:
                        await self.conn.execute('DELETE FROM bot_meta WHERE key=$1', key)
                    elif key in ('diagnostics', 'services'):
                        previous = decode(row['value'])
                        if not isinstance(previous, dict):
                            continue
                        current = {name: value for name, value in previous.items()
                                   if not obsolete(('SERVICE:' if key == 'services' else '') + name)}
                        if current != previous:
                            await self.conn.execute(
                                'UPDATE bot_meta SET value=$2::jsonb WHERE key=$1', key, json.dumps(current))
                # This prefix guard is mandatory: A/B bodies can contain arbitrary text.
                for row in await self.conn.fetch("SELECT id,body FROM bot_pending WHERE id LIKE 'diag:%'"):
                    if not row['id'].startswith('diag:'):
                        continue
                    current = cleaned_body(row['body'])
                    if current is None:
                        await self.conn.execute('DELETE FROM bot_pending WHERE id=$1', row['id'])
                    elif current != row['body']:
                        await self.conn.execute('UPDATE bot_pending SET body=$2 WHERE id=$1', row['id'], current)
