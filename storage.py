"""Bounded PostgreSQL snapshots and transactional durable outbox."""
import asyncio
import json
import time

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
CREATE INDEX IF NOT EXISTS bot_pending_due ON bot_pending(due_ms, detected_ms);
"""


class Backpressure(RuntimeError):
    pass


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

    async def load(self):
        async with self.lock:
            rows = await self.conn.fetch('SELECT s.symbol,s.state,c.candles FROM bot_symbols s LEFT JOIN bot_candles c USING(symbol)')
            meta = await self.conn.fetch('SELECT key,value FROM bot_meta')
        decode = lambda v: json.loads(v) if isinstance(v, str) else v
        states = {r['symbol']: {**decode(r['state']), 'candles': decode(r['candles']) if r['candles'] else []} for r in rows}
        self.history_revisions = {symbol: value.get('history_revision', 0) for symbol, value in states.items()}
        return states, {
            r['key']: decode(r['value']) for r in meta}

    async def count_pending(self):
        async with self.lock:
            return await self.conn.fetchval('SELECT count(*) FROM bot_pending')

    async def commit(self, states=(), events=(), meta=None, delete=()):
        """Persist event flags and pending messages in the SAME transaction."""
        stamp = int(time.time() * 1000)
        async with self.lock:
            async with self.conn.transaction():
                count = await self.conn.fetchval('SELECT count(*) FROM bot_pending')
                if count + len(events) > self.max_pending:
                    raise Backpressure('Outbox full; new detection paused')
                for symbol, payload in states:
                    payload = dict(payload)
                    candles = payload.pop('candles', [])
                    if self.history_revisions.get(symbol) != payload.get('history_revision', 0):
                        await self.conn.execute('INSERT INTO bot_candles VALUES($1,$2::jsonb) ON CONFLICT(symbol) DO UPDATE SET candles=EXCLUDED.candles', symbol, json.dumps(candles))
                    await self.conn.execute(
                        'INSERT INTO bot_symbols VALUES($1,$2::jsonb,$3) '
                        'ON CONFLICT(symbol) DO UPDATE SET state=EXCLUDED.state, '
                        'updated_at=EXCLUDED.updated_at', symbol, json.dumps(payload), stamp)
                for event in events:
                    await self.conn.execute(
                        'INSERT INTO bot_pending(id,symbol,body,detected_ms) VALUES($1,$2,$3,$4) '
                        'ON CONFLICT(id) DO NOTHING', event.id, event.symbol,
                        event.message, event.detected_ms)
                for key, value in (meta or {}).items():
                    await self.conn.execute(
                        'INSERT INTO bot_meta VALUES($1,$2::jsonb) ON CONFLICT(key) '
                        'DO UPDATE SET value=EXCLUDED.value', key, json.dumps(value))
                for symbol in delete:
                    # Pending detected signals deliberately survive delisting.
                    await self.conn.execute('DELETE FROM bot_symbols WHERE symbol=$1', symbol)
                    await self.conn.execute('DELETE FROM bot_candles WHERE symbol=$1', symbol)
            for symbol, payload in states:
                self.history_revisions[symbol] = payload.get('history_revision', 0)
            for symbol in delete:
                self.history_revisions.pop(symbol, None)

    async def first_pending(self, now_ms):
        async with self.lock:
            row = await self.conn.fetchrow(
                'SELECT * FROM bot_pending WHERE due_ms <= $1 '
                'ORDER BY detected_ms,id LIMIT 1', now_ms)
        return dict(row) if row else None

    async def complete(self, event_id):
        async with self.lock:
            await self.conn.execute('DELETE FROM bot_pending WHERE id=$1', event_id)

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
