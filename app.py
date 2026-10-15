"""One-process Binance public-market observer. No trading or public server."""
import asyncio
import copy
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import logging
import os
import random
import time
from uuid import uuid4
import aiohttp
from api import ApiFault, Binance, Telegram, delayed_message, retry_delay
from engine import State, Candle, Event, evaluate, problems, to_dict, from_dict, minimum_close, FOUR_HOURS_MS, DAY_MS
from rules import resolve_funding
from storage import Store, Backpressure

LOG = logging.getLogger('funding_bot')
KST = timezone(timedelta(hours=9))
EVENT_FIELDS = ('known_interval', 'previous_interval', 'a_epoch', 'a_sent',
                'b_epoch', 'b_sent', 'b_floor', 'recovery_started_ms', 'recovery_last_ms')


def accepted_contract(row):
    return isinstance(row, dict) and all(row.get(key) == expected for key, expected in (
        ('status', 'TRADING'), ('contractType', 'PERPETUAL'),
        ('marginAsset', 'USDT'), ('quoteAsset', 'USDT')))


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        d = Decimal(str(value))
        return d if d.is_finite() else None
    except (ValueError, TypeError, InvalidOperation):
        return None


def timestamp(value):
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (ValueError, TypeError, OverflowError):
        return 0


def signature(state):
    return tuple(getattr(state, key) for key in EVENT_FIELDS)


LIVE_FIELDS = frozenset({'price', 'price_ms', 'funding', 'funding_ms', 'next_funding_ms'})
FINGERPRINT_SKIP = LIVE_FIELDS | frozenset({'interval_ms', 'floor_ms', 'last_official_floor_ms',
    'history_min', 'history_min_close_ms', 'history_cache_revision', 'history_cache_count',
    'history_cache_last_ms', 'history_cache_ready', 'history_cache_closed_ms'})


def compact_state(state):
    result = {}
    for f in fields(state):
        if f.name != 'candles' and f.name not in LIVE_FIELDS:
            value = getattr(state, f.name)
            result[f.name] = str(value) if isinstance(value, Decimal) else value
    return result


def durable_signature(state):
    payload = {k: v for k, v in compact_state(state).items() if k not in FINGERPRINT_SKIP}
    # A fresh observed-history estimate must survive restart, unlike per-poll official clocks.
    if state.floor_estimated:
        payload['floor_ms'] = state.floor_ms
    return json.dumps(payload, sort_keys=True, separators=(',', ':'))


class Bot:
    def __init__(self, store, session, token, chat_id, health_url=''):
        self.store, self.session = store, session
        self.binance = Binance(session)
        self.telegram = Telegram(session, token, chat_id)
        self.health_url = health_url
        self.states, self.meta, self.rows = {}, {}, {}
        self.lock = asyncio.Lock()
        self.ready = False
        self.pending_full = False
        self.services = {}
        self.offset_ms = 0
        self.started_ms = int(time.time() * 1000)
        self.last_stream = {'mark': 0, 'price': 0}
        self.adjustments = None
        self.adjustments_ms = 0
        self.histories, self.history_checked, self.candle_retry = {}, {}, {}
        self.kline_gate = asyncio.Semaphore(2)
        self.saved_signatures = {}
        self.history_progress_ms = 0
        self.funding_progress_ms = 0
        self.funding_history_inflight = set()
        self.history_inflight = set()

    def now(self):
        return int(time.time() * 1000) + self.offset_ms

    async def save(self, states=(), events=(), meta=None, delete=(), candles=False):
        if not self.ready:
            raise RuntimeError('Database not ready')
        states, events, delete = list(states), list(events), list(delete)
        dirty = [s for s in states if self.saved_signatures.get(s.symbol) != durable_signature(s)]
        changed_meta = {k: v for k, v in (meta or {}).items() if self.meta.get(k) != v}
        if not dirty and not events and not changed_meta and not delete:
            return
        payloads = []
        for state in dirty:
            payload = compact_state(state)
            if candles:
                payload['candles'] = to_dict(state)['candles']
            payloads.append((state.symbol, payload))
        try:
            await self.store.commit(payloads, events, changed_meta, delete)
        except Backpressure:
            self.pending_full = True
            raise
        except Exception:
            self.ready = False
            LOG.error('Database unavailable; detection and sender paused')
            raise
        for state in dirty:
            self.saved_signatures[state.symbol] = durable_signature(state)
        for symbol in delete:
            self.saved_signatures.pop(symbol, None)
        self.meta.update(copy.deepcopy(changed_meta))

    async def connect_db(self):
        async with self.lock:
            await self.store.close()
            await self.store.open()
            migrate = getattr(self.store, 'retire_legacy_diagnostics', None)
            if migrate:
                await migrate()
            payloads, self.meta = await self.store.load()
            self.states = {key: from_dict(value) for key, value in payloads.items()}
            self.saved_signatures = {key: durable_signature(state) for key, state in self.states.items()}
            verified = self.meta.get('funding_checkpoint_ms', 0)
            for s in self.states.values():
                if s.interval_source.startswith('OFFICIAL_'):
                    s.interval_ms = max(s.interval_ms, verified)
                if s.floor_source.startswith('OFFICIAL_'):
                    s.floor_ms = max(s.floor_ms, verified)
                    if s.last_official_floor == s.floor:
                        s.last_official_floor_ms = max(s.last_official_floor_ms, verified)
                s.price = s.funding = None
                s.price_ms = s.funding_ms = 0
                s.recovery_started_ms = s.recovery_last_ms = None
            self.rows = self.meta.get('exchange_rows', {})
            self.services = copy.deepcopy(self.meta.get('services', {}))
            self.pending_full = await self.store.count_pending() >= self.store.max_pending
            self.ready = True
            LOG.info('Database ready: restored %d symbols and %d candle histories', len(self.states), sum(bool(state.candles) for state in self.states.values()))

    async def db_loop(self):
        while True:
            try:
                if not self.ready:
                    await self.connect_db()
                else:
                    self.pending_full = await self.store.count_pending() >= self.store.max_pending
            except Exception as exc:
                self.ready = False
                LOG.error('Database reconnect: %s', type(exc).__name__)
            await asyncio.sleep(5)

    async def service_problem(self, key, detail=None):
        async with self.lock:
            old = dict(self.services)
            if detail is None:
                self.services.pop(key, None)
            else:
                self.services[key] = detail
            if old != self.services and self.ready:
                try:
                    await self.save(meta={'services': self.services})
                except Exception:
                    self.services = old

    async def repeat(self, key, work, seconds):
        delay = seconds
        while True:
            if not self.ready:
                await asyncio.sleep(2)
                continue
            begin = time.monotonic()
            try:
                await work()
                await self.service_problem(key)
                delay = seconds
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                safe = type(exc).__name__
                if isinstance(exc, ApiFault):
                    safe += f' HTTP {exc.status}'
                await self.service_problem(key, safe)
                LOG.warning('%s: %s', key, safe)
                delay = max(min(seconds, 60), min(delay * 2, 1800), getattr(exc, 'delay', 0))
            await asyncio.sleep(max(.1, delay - (time.monotonic() - begin)))

    async def assess(self, symbols):
        if not self.ready or self.pending_full:
            return
        evaluated, events = {}, []
        for symbol in dict.fromkeys(symbols):
            old = self.states.get(symbol)
            if old is not None:
                state = copy.copy(old)
                events.extend(evaluate(state, self.now()))
                evaluated[symbol] = state
        await self.save(evaluated.values(), events)
        self.states.update(evaluated)

    async def discover(self):
        payload = await self.binance.get('/exchangeInfo')
        if not isinstance(payload, dict) or not isinstance(payload.get('symbols'), list):
            raise ApiFault(0, 60, 'Invalid exchange metadata')
        if not payload['symbols']:
            raise ApiFault(0, 60, 'Empty exchange metadata; retaining known symbols')
        keep = ('symbol', 'baseAsset', 'quoteAsset', 'marginAsset', 'contractType', 'status', 'onboardDate')
        all_rows = {r['symbol']: {k: r.get(k) for k in keep} for r in payload['symbols']
                    if isinstance(r, dict) and r.get('symbol') and r.get('contractType') == 'PERPETUAL'
                    and r.get('quoteAsset') == 'USDT' and r.get('marginAsset') == 'USDT'}
        rows = {key: row for key, row in all_rows.items() if accepted_contract(row)}
        now = self.now()
        async with self.lock:
            if not self.ready:
                return
            retired = copy.deepcopy(self.meta.get('retired_symbols', {}))
            fresh, changed = {}, []
            for symbol, old in self.states.items():
                if symbol in rows:
                    continue
                state = copy.copy(old)
                state.trading = False
                fresh[symbol] = state
                changed.append(state)
                raw = all_rows.get(symbol)
                if (raw is None or raw.get('status') in ('CLOSE', 'DELIVERED', 'PRE_DELIVERING')) and symbol not in retired:
                    retired[symbol] = {'onboard_ms': old.source_onboard_ms or old.onboard_ms,
                        'observed_ms': now, 'status': (raw or {}).get('status', 'REMOVED')}
            for symbol, row in rows.items():
                raw_onboard = timestamp(row.get('onboardDate'))
                source = raw_onboard if 0 < raw_onboard <= now else 0
                old = self.states.get(symbol)
                retired_listing = retired.get(symbol)
                previous_status = (self.rows.get(symbol) or {}).get('status')
                explicit_relist = bool(retired_listing or (old and previous_status == 'PRE_DELIVERING'))
                unresolved_relist = bool(old and old.listing_status == 'RELISTING_TIME_UNKNOWN')
                evidence_ms = ((retired_listing or {}).get('observed_ms', 0)
                    or (old.listing_evidence_ms if old else 0)
                    or (self.meta.get('exchange_checked_ms', now) if explicit_relist else 0))
                previous_onboard = ((retired_listing or {}).get('onboard_ms', 0)
                    or (old.source_onboard_ms or old.onboard_ms if old else 0))
                if explicit_relist or unresolved_relist:
                    verified = bool(source and source > previous_onboard and source >= evidence_ms)
                    onboard = source if verified else 0
                    listing_status = 'CONFIRMED' if verified else 'RELISTING_TIME_UNKNOWN'
                else:
                    onboard = source
                    listing_status = 'CONFIRMED' if source else 'LISTING_TIME_UNKNOWN'
                replacement = (old is None or explicit_relist
                    or (old.source_onboard_ms or old.onboard_ms) != source)
                if replacement:
                    state = State(symbol, str(row.get('baseAsset') or symbol), onboard)
                    state.source_onboard_ms = source
                    state.listing_status = listing_status
                    state.listing_evidence_ms = evidence_ms
                    if old is not None:
                        state.candles = old.candles
                        state.history_revision = old.history_revision
                else:
                    state = copy.copy(old)
                    if state.onboard_ms != onboard:
                        state.history_ready = False
                        state.history_revision += 1
                        state.history_cache_revision = -1
                    state.onboard_ms = onboard
                    state.listing_status = listing_status
                    state.listing_evidence_ms = evidence_ms
                    state.base = str(row.get('baseAsset') or symbol)
                state.trading = True
                retired.pop(symbol, None)
                # Routine moving-window retention; inactive operational records are untouched.
                retained = [c for c in state.candles if c.close_ms >= now - 90 * DAY_MS]
                if len(retained) != len(state.candles):
                    state.candles = retained
                    state.history_revision += 1
                fresh[symbol] = state
                changed.append(state)
            retired = {key: value for key, value in retired.items() if now - value.get('observed_ms', 0) <= 365 * DAY_MS}
            retired = dict(sorted(retired.items(), key=lambda item: item[1].get('observed_ms', 0), reverse=True)[:2000])
            meta = {'exchange_rows': all_rows, 'exchange_checked_ms': now, 'retired_symbols': retired,
                'universe_stats': {'total': len(all_rows), 'trading': len(rows)}, 'initialized': True}
            # No operational symbol/candle/event deletion during v2 -> v3 migration.
            await self.save(changed, meta=meta, candles=True)
            self.states, self.rows = fresh, all_rows
            active = set(rows)
            self.candle_retry = {key: value for key, value in self.candle_retry.items() if key in active}
            self.histories = {key: value for key, value in self.histories.items() if key in active}
            self.history_checked = {key: value for key, value in self.history_checked.items() if key in active}
            self.services = {key: value for key, value in self.services.items() if not key.startswith('KLINES:') or key[7:] in active}
            await self.assess(rows)
            LOG.info('Universe ready: %d eligible USDT-M perpetual contracts, %d retained inactive records', len(rows), len(fresh) - len(rows))

    async def sync_clock(self):
        before = int(time.time() * 1000)
        payload = await self.binance.get('/time')
        after = int(time.time() * 1000)
        server = timestamp(payload.get('serverTime')) if isinstance(payload, dict) else 0
        if server <= 0 or after - before > 5000:
            raise ApiFault(0, 60, 'Clock sync unavailable')
        self.offset_ms = server - (before + after) // 2

    async def refresh_funding(self):
        adjustments = await self.binance.get('/fundingInfo')
        if not isinstance(adjustments, list) or any(not isinstance(r, dict) or not r.get('symbol') for r in adjustments):
            raise ApiFault(0, 60, 'Invalid fundingInfo')
        self.adjustments = {row['symbol']: row for row in adjustments}
        self.adjustments_ms = self.now()
        self.funding_progress_ms = self.now()
        async with self.lock:
            updated, events = {}, []
            for old in self.states.values():
                state = copy.copy(old)
                self.apply_rule(state)
                if not self.pending_full:
                    events.extend(evaluate(state, self.now()))
                updated[state.symbol] = state
            # One shared verification timestamp; no unchanged symbol snapshots.
            meta = {}
            if self.now() - self.meta.get('funding_checkpoint_ms', 0) >= 60_000:
                meta['funding_checkpoint_ms'] = self.adjustments_ms
            await self.save(updated.values(), events, meta=meta)
            self.states.update(updated)

    def apply_rule(self, state):
        now = self.now()
        fresh = self.adjustments if 0 <= now - self.adjustments_ms < 300_000 else None
        previous = {'last_official_floor': str(state.last_official_floor) if state.last_official_floor else None,
                    'last_official_floor_ms': state.last_official_floor_ms,
                    'last_official_floor_source': state.last_official_floor_source}
        rule = resolve_funding(state.symbol, fresh, self.histories.get(state.symbol, []), state.next_times, now, previous)
        state.interval_hours, state.interval_source = rule['interval'], rule['interval_source']
        state.interval_ms = now if state.interval_hours else 0
        state.floor, state.floor_source = number(rule['floor']), rule['floor_source']
        state.floor_estimated, state.floor_ms = rule['floor_estimated'], rule['floor_ms']
        state.last_official_floor = number(rule['last_official_floor'])
        state.last_official_floor_ms = rule['last_official_floor_ms']
        state.last_official_floor_source = rule['last_official_floor_source'] or ''
        if state.interval_source.startswith('OFFICIAL_'):
            state.interval_ms = self.adjustments_ms
        if state.floor_source.startswith('OFFICIAL_'):
            state.floor_ms = self.adjustments_ms
            state.last_official_floor_ms = self.adjustments_ms
        elif state.floor_estimated:
            state.floor_ms = self.history_checked.get(state.symbol, 0)

    async def supplement(self):
        try:
            await self._supplement()
        finally:
            self.funding_history_inflight.clear()

    async def _supplement(self):
        now = self.now()
        wanted = [s for s in self.states.values() if s.trading
                  and (s.floor is None or s.floor_estimated or s.interval_hours is None)
                  and now - self.history_checked.get(s.symbol, 0) >= DAY_MS]
        self.funding_history_inflight.update(state.symbol for state in wanted)
        for old in wanted:
            self.funding_progress_ms = self.now()
            records, cursor = [], max(old.onboard_ms, now - 30 * DAY_MS)
            while cursor < now:
                page = await self.binance.get('/fundingRate', {'symbol': old.symbol, 'startTime': cursor, 'endTime': now, 'limit': 1000})
                if not isinstance(page, list) or any(not isinstance(r, dict) or r.get('symbol', old.symbol) != old.symbol for r in page):
                    raise ApiFault(0, 60, 'Invalid funding history')
                records.extend(page)
                if len(page) < 1000:
                    break
                next_cursor = max(timestamp(r.get('fundingTime')) for r in page) + 1
                if next_cursor <= cursor or len(records) > 1000:
                    raise ApiFault(0, 60, 'Invalid history pagination')
                cursor = next_cursor
            self.histories[old.symbol], self.history_checked[old.symbol] = records[-750:], self.now()
            self.funding_history_inflight.discard(old.symbol)
            self.funding_progress_ms = self.now()
            async with self.lock:
                s = self.states.get(old.symbol)
                if s:
                    new = copy.copy(s)
                    self.apply_rule(new)
                    await self.save([new])
                    self.states[s.symbol] = new
                    await self.assess([s.symbol])
        self.histories = {k: v for k, v in self.histories.items() if k in self.states}
        self.history_checked = {k: v for k, v in self.history_checked.items() if k in self.states}

    async def fetch_candles(self, old):
        async with self.kline_gate:
            now = self.now()
            lower = max(old.onboard_ms, now - 90 * DAY_MS)
            first = lower // FOUR_HOURS_MS * FOUR_HOURS_MS
            last = now // FOUR_HOURS_MS * FOUR_HOURS_MS - FOUR_HOURS_MS
            # Reuse every valid stored candle; request the first contiguous missing span.
            candles = {c.open_ms: c for c in old.candles if c.close_ms >= now - 90 * DAY_MS
                and c.close_ms < now and c.close.is_finite() and c.close > 0
                and c.close_ms == c.open_ms + FOUR_HOURS_MS - 1 and c.open_ms % FOUR_HOURS_MS == 0}
            start = next((opened for opened in range(first, last + 1, FOUR_HOURS_MS) if opened not in candles), None)
            if start is None:
                # A legacy not-ready flag may coexist with a complete stored history.
                async with self.lock:
                    state = self.states.get(old.symbol)
                    if state is None or state.onboard_ms != old.onboard_ms or not self.ready:
                        return
                    updated = copy.copy(state)
                    updated.history_ready = True
                    updated.history_cache_revision = -1
                    if minimum_close(updated, self.now()) is not None:
                        await self.save([updated])
                        self.states[state.symbol] = updated
                        await self.assess([state.symbol])
                return
            end = start
            while end + FOUR_HOURS_MS <= last and end + FOUR_HOURS_MS not in candles:
                end += FOUR_HOURS_MS
            count = (end - start) // FOUR_HOURS_MS + 1
            limit = min(1000, max(2, count + 1))
            weight = 1 if limit < 100 else 2 if limit < 500 else 5
            self.history_progress_ms = self.now()
            page = await self.binance.get('/klines', {'symbol': old.symbol, 'interval': '4h',
                'startTime': start, 'endTime': end + FOUR_HOURS_MS - 1, 'limit': limit}, weight=weight)
            if not isinstance(page, list):
                raise ApiFault(0, 60, 'Invalid candle data')
            for row in page:
                if not isinstance(row, list) or len(row) < 7:
                    raise ApiFault(0, 60, 'Invalid candle row')
                opened, closed, close = timestamp(row[0]), timestamp(row[6]), number(row[4])
                if close is None or close <= 0 or closed != opened + FOUR_HOURS_MS - 1 or opened % FOUR_HOURS_MS:
                    raise ApiFault(0, 60, 'Invalid closed candle')
                if now - 90 * DAY_MS <= closed < now:
                    candles[opened] = Candle(opened, closed, close)
            async with self.lock:
                state = self.states.get(old.symbol)
                if state is None or state.onboard_ms != old.onboard_ms or not self.ready:
                    return
                updated = copy.copy(state)
                updated.candles = sorted(candles.values(), key=lambda candle: candle.open_ms)[-541:]
                updated.history_ready = True
                updated.history_revision += 1
                if minimum_close(updated, self.now()) is None:
                    updated.history_ready = False
                    self.candle_retry[state.symbol] = self.now() + 60_000
                else:
                    self.candle_retry.pop(state.symbol, None)
                await self.save([updated], candles=True)
                self.states[state.symbol] = updated
                self.history_progress_ms = self.now()
                await self.assess([state.symbol])

    async def candle_loop(self):
        while True:
            now = self.now()
            if self.ready:
                wanted = [state for state in self.states.values() if state.trading
                    and state.onboard_ms > 0 and now >= state.onboard_ms + 168 * 3_600_000
                    and self.candle_retry.get(state.symbol, 0) <= now
                    and minimum_close(state, now) is None]
                async def one(state):
                    self.history_inflight.add(state.symbol)
                    try:
                        await self.fetch_candles(state)
                        await self.service_problem('KLINES:' + state.symbol)
                    except Exception as exc:
                        self.candle_retry[state.symbol] = self.now() + max(60_000, int(getattr(exc, 'delay', 60) * 1000))
                        await self.service_problem('KLINES:' + state.symbol, type(exc).__name__ + (f' HTTP {exc.status}' if isinstance(exc, ApiFault) else ''))
                    finally:
                        self.history_inflight.discard(state.symbol)
                await asyncio.gather(*(one(state) for state in wanted))
            await asyncio.sleep(1)

    async def ingest(self, stream, payload, track_stream=True):
        if not isinstance(payload, list):
            raise ApiFault(0, 10, 'Invalid WebSocket array')
        now, affected, valid_rows = self.now(), [], 0
        async with self.lock:
            for row in payload:
                if not isinstance(row, dict) or not isinstance(row.get('s'), str):
                    continue
                expected = 'markPriceUpdate' if stream == 'mark' else '24hrMiniTicker'
                if row.get('e') not in (expected, None):
                    continue
                stamp = timestamp(row.get('E'))
                val = number(row.get('r' if stream == 'mark' else 'c'))
                max_age = 15_000 if stream == 'mark' else 120_000
                if not stamp or not 0 <= now - stamp < max_age or val is None or (stream == 'price' and val <= 0):
                    continue
                if stream == 'mark' and timestamp(row.get('T')) <= stamp:
                    continue
                valid_rows += 1
                s = self.states.get(row['s'])
                if s is None:
                    continue
                if stream == 'mark' and stamp > s.funding_ms:
                    next_ms = timestamp(row.get('T'))
                    if next_ms <= stamp:
                        continue
                    s.funding, s.funding_ms, s.next_funding_ms = val, stamp, next_ms
                    if not s.next_times or s.next_times[-1] != next_ms:
                        s.next_times = (s.next_times + [next_ms])[-8:]
                        self.apply_rule(s)
                    affected.append(s.symbol)
                elif stream == 'price' and stamp > s.price_ms:
                    s.price, s.price_ms = val, stamp
                    affected.append(s.symbol)
            if valid_rows and track_stream:
                self.last_stream[stream] = now
            await self.assess(affected)
        if not valid_rows:
            raise ApiFault(0, 10, 'No valid fresh stream rows')

    async def stream_loop(self, kind):
        stream = '!markPrice@arr@1s' if kind == 'mark' else '!miniTicker@arr'
        url = 'wss://fstream.binance.com/market/ws/' + stream
        timeout = 15 if kind == 'mark' else 120
        backoff = 1
        while True:
            try:
                async with self.session.ws_connect(url, heartbeat=20, max_msg_size=4 * 1024 * 1024) as ws:
                    announced = False
                    while True:
                        message = await asyncio.wait_for(ws.receive(), timeout=timeout)
                        if message.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(message.data, parse_float=Decimal)
                            except ValueError:
                                raise ApiFault(0, 10, 'Malformed WebSocket JSON') from None
                            await self.ingest(kind, payload)
                            if not announced:
                                LOG.info('%s stream receiving fresh data', kind)
                                announced = True
                            await self.service_problem('WS:' + kind)
                            backoff = 1
                        elif message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                            raise ApiFault(0, 10, 'WebSocket disconnected')
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                status = getattr(exc, 'status', 0)
                if status in (403, 451):
                    pause = 1800
                elif status == 429:
                    pause = max(60, backoff, retry_delay(getattr(exc, 'headers', {}).get('Retry-After')))
                else:
                    pause = backoff
                await self.service_problem('WS:' + kind, f'{type(exc).__name__} HTTP {status}')
                await asyncio.sleep(pause + random.random())
                backoff = min(60, backoff * 2)

    async def reconcile_live(self):
        now = self.now()
        if now - self.last_stream['mark'] >= 15_000 or any(s.trading and now - s.funding_ms >= 15_000 for s in self.states.values()):
            data = await self.binance.get('/premiumIndex', weight=10)
            if not isinstance(data, list):
                raise ApiFault(0, 60, 'Invalid premiumIndex')
            await self.ingest('mark', [{'s': r.get('symbol'), 'E': r.get('time'),
                'r': r.get('lastFundingRate'), 'T': r.get('nextFundingTime')} for r in data if isinstance(r, dict)], track_stream=False)
        if now - self.last_stream['price'] >= 120_000 or any(s.trading and now - s.price_ms >= 120_000 for s in self.states.values()):
            data = await self.binance.prices()
            if not isinstance(data, list):
                raise ApiFault(0, 60, 'Invalid actual price ticker')
            await self.ingest('price', [{'s': r.get('symbol'), 'E': r.get('time'),
                                       'c': r.get('price')} for r in data if isinstance(r, dict)], track_stream=False)
        async with self.lock:
            for state in self.states.values():
                self.apply_rule(state)
            await self.assess(self.states)

    def initialization_active(self):
        history = self.history_inflight and 0 <= self.now() - self.history_progress_ms < 120_000
        funding = self.funding_history_inflight and 0 <= self.now() - self.funding_progress_ms < 120_000
        return bool(history or funding)

    def symbol_problems(self, state):
        found = problems(state, self.now())
        if state.listing_status == 'RELISTING_TIME_UNKNOWN':
            found['LISTING_UNKNOWN'] = '재상장은 확인됐지만 실제 재상장시각이 미확인입니다. A/B를 보류합니다.'
        return found

    @staticmethod
    def diagnostic_group(key, value):
        if key.startswith('SERVICE:'):
            raw_service = key[len('SERVICE:'):]
            service = 'KLINES' if raw_service.startswith('KLINES:') else raw_service
            # An access block replacing a rate-limit/outage is a new important fault.
            detail = str(value).strip()[:120]
            return 'SERVICE:' + service + ':' + detail
        return key.split(':')[-1]

    async def diagnostics(self):
        async with self.lock:
            previous = self.meta.get('diagnostics', {})
            previous_types = {self.diagnostic_group(key, value) for key, value in previous.items()}
            current = {'SERVICE:' + key: value for key, value in self.services.items()}
            warming = 0 <= self.now() - self.started_ms < 60_000
            collecting = self.initialization_active()
            for state in self.states.values():
                for key, value in self.symbol_problems(state).items():
                    # Normal history/funding initialization is visible in /status.
                    pending_live = ((key == 'FUNDING_INVALID' and state.funding_ms == 0)
                        or (key == 'PRICE_INVALID' and state.price_ms == 0)
                        or (key == 'INTERVAL_UNKNOWN' and self.adjustments_ms == 0))
                    previously_active = self.diagnostic_group(state.symbol + ':' + key, value) in previous_types
                    if not previously_active:
                        if pending_live and (warming or collecting):
                            continue
                        if key == 'PRICE_HISTORY_UNKNOWN' and (collecting or warming or state.symbol in self.history_inflight):
                            continue
                    current[state.symbol + ':' + key] = value
            if self.pending_full:
                current['OUTBOX_FULL'] = '미전송 큐가 가득 차 새 이벤트 감지를 일시 보류합니다.'
            previous = self.meta.get('diagnostics', {})
            previous_types = {self.diagnostic_group(key, value) for key, value in previous.items()}
            grouped = {}
            for key, value in current.items():
                grouped.setdefault(self.diagnostic_group(key, value), []).append(key)
            new_types = sorted(set(grouped) - previous_types,
                key=lambda key: (not key.startswith('SERVICE:'), key))
            events = []
            if new_types:
                lines = []
                for kind in new_types:
                    members = grouped[kind]
                    line = f'{kind}: {len(members)}건'
                    if len(members) <= 3 and not kind.startswith('SERVICE:'):
                        line += ' (' + ', '.join(members) + ')'
                    lines.append(line)
                stamp = self.now()
                detected = datetime.fromtimestamp(stamp / 1000, KST).strftime('%Y-%m-%d %H:%M:%S KST')
                body = f'진단 요약 · {detected}\n' + '\n'.join(lines)
                body += '\n종목별 상세: /status SYMBOL · 미확인 목록: /status 1 (다음 페이지는 숫자 증가)'
                # The number of problem types is bounded, independent of symbol count.
                if len(body) > 3800:
                    body = f'진단 요약 · {detected}\n새 장애 유형 {len(new_types)}개, 영향 상태 {sum(len(grouped[k]) for k in new_types)}건\n'
                    body += '\n'.join(line[:150] for line in lines[:15])
                    body += '\n모든 상세 유형과 종목은 /status 및 /status 1에서 확인하십시오.'
                events = [Event('diag:' + str(uuid4()), '', 'DIAGNOSTIC', stamp, body)]
            if current != previous:
                await self.save(events=events, meta={'diagnostics': current})

    async def sender_loop(self):
        while True:
            if not self.ready:
                await asyncio.sleep(2)
                continue
            try:
                row = await self.store.first_pending(self.now())
                if not row:
                    await asyncio.sleep(1)
                    continue
                attempt = row['attempts'] + 1
                delay = min(3600, 5 * 2 ** min(attempt, 10))
                await self.store.retry(row['id'], attempt, self.now() + delay * 1000)
                try:
                    await self.telegram.send(delayed_message(row, self.now()))
                except Exception as exc:
                    delay = max(delay, getattr(exc, 'delay', 0))
                    await self.store.retry(row['id'], attempt, self.now() + int(delay * 1000))
                    await self.service_problem('TELEGRAM', type(exc).__name__)
                else:
                    await self.store.complete(row['id'])
                    await self.service_problem('TELEGRAM')
                await asyncio.sleep(1.1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ready = False
                LOG.error('Sender database failure: %s', type(exc).__name__)
                await asyncio.sleep(5)

    def status_problems(self, state):
        found = self.symbol_problems(state)
        if not state.trading:
            found['NOT_TRADING'] = '현재 거래 중인 적격 USDT-M 무기한 계약이 아닙니다.'
        elif state.onboard_ms > 0 and 0 <= self.now() - state.onboard_ms < 168 * 3_600_000:
            found['LISTING_WAIT'] = '실제 상장 후 168시간이 지나지 않았습니다.'
        return found

    async def status(self, query=''):
        pending = await self.store.count_pending() if self.ready else 'DB 연결 없음'
        now = self.now()
        active = [state for state in self.states.values() if state.trading]
        raw = {state.symbol: self.status_problems(state) for state in active}
        query = str(query).strip().upper()
        if query and not query.isdecimal():
            symbol = query if query.endswith('USDT') else query + 'USDT'
            state = self.states.get(symbol)
            if state is None:
                return f'{symbol}: 현재 감시 후보에 없습니다.'
            lines = [f'감시 봇 v3 · {symbol}', f'상장 상태: {state.listing_status}',
                     f'실제 상장시각(ms): {state.onboard_ms or "미확인"}']
            for key, value in self.status_problems(state).items():
                lines.append(f'{key}: {value}')
            if len(lines) == 3:
                lines.append('필수 데이터 준비 완료. 가격 3배·예상 음수 펀딩비 조건은 별도 검사합니다.')
            if self.initialization_active():
                lines.append('가격 이력·펀딩 데이터 초기 수집 진행 중')
            return '\n'.join(lines)
        unresolved = [(symbol, found) for symbol, found in sorted(raw.items()) if found]
        if query:
            page, size = max(1, int(query)), 15
            pages = max(1, (len(unresolved) + size - 1) // size)
            items = unresolved[(page - 1) * size:page * size]
            lines = [f'v3 미준비 종목 {len(unresolved)}개 · {page}/{pages}페이지']
            lines.extend(symbol + ': ' + ', '.join(found) for symbol, found in items)
            if not items:
                lines.append('해당 페이지의 미준비 종목이 없습니다.')
            lines.append(f'다음: /status {page + 1} · 상세: /status SYMBOL')
            return '\n'.join(lines)
        universe = self.meta.get('universe_stats', {})
        total = universe.get('total', len(self.rows) or len(active))
        candidates = len(active)
        waiting = sum(state.onboard_ms > 0 and 0 <= now - state.onboard_ms < 168 * 3_600_000 for state in active)
        listing_unknown = sum(state.onboard_ms <= 0 or state.listing_status != 'CONFIRMED' for state in active)
        data_ready = sum(not found for found in raw.values())
        ready = data_ready if self.ready and not self.pending_full else 0
        types = {}
        for found in raw.values():
            for key in found:
                types[key] = types.get(key, 0) + 1
        lines = ['감시 봇 v3 /status', f'DB: {"정상" if self.ready else "재연결 중"}',
            f'전체 USDT-M 무기한: {total}', f'현재 감시 후보: {candidates}',
            f'상장 7일 대기: {waiting}', f'상장시각 미확인: {listing_unknown}',
            f'실제 감시 준비 완료: {ready}', f'미준비 종목: {candidates - ready}',
            '준비 수는 필수 데이터 기준이며 가격 3배·음수 조건과 별개입니다.',
            '미준비 사유는 중복 집계하므로 사유별 합계는 미준비 종목 수와 다를 수 있습니다.',
            f'가격 이력 준비: {sum(state.history_ready and minimum_close(state, now) is not None for state in active)}/{candidates}',
            f'초기 수집 진행: {self.initialization_active()}', f'미전송: {pending}',
            f'펀딩 WS: {max(0, (now - self.last_stream["mark"]) // 1000)}초 전',
            f'가격 WS: {max(0, (now - self.last_stream["price"]) // 1000)}초 전',
            f'새 감지 보류: {self.pending_full}']
        if types:
            lines.append('미준비 유형: ' + ', '.join(f'{key}={count}' for key, count in sorted(types.items())))
        if self.services:
            lines.append('서비스 장애: ' + ', '.join(sorted({'KLINES' if key.startswith('KLINES:') else key for key in self.services})))
        lines.append('상세: /status SYMBOL · 미준비 목록: /status 1 (페이지당 15종목)')
        return '\n'.join(lines)

    async def commands_loop(self):
        while not self.ready:
            await asyncio.sleep(1)
        offset = int(self.meta.get('telegram_offset', 0))
        poll_ready = False
        while True:
            try:
                updates = await self.telegram.updates(offset)
                if not isinstance(updates, list):
                    raise ApiFault(0, 60, 'Invalid Telegram updates')
                if not poll_ready:
                    LOG.info('Telegram long polling ready')
                    poll_ready = True
                for update in updates:
                    if not isinstance(update, dict):
                        continue
                    offset = max(offset, timestamp(update.get('update_id')) + 1)
                    msg = update.get('message') or {}
                    text = str(msg.get('text', '')).split()
                    if (str((msg.get('chat') or {}).get('id')) == self.telegram.chat_id
                            and text and text[0].split('@')[0] == '/status'):
                        await self.telegram.send(await self.status(' '.join(text[1:])))
                        LOG.info('Status request served by v3')
                if updates and self.ready:
                    async with self.lock:
                        await self.save(meta={'telegram_offset': offset})
                        self.meta['telegram_offset'] = offset
                await self.service_problem('TELEGRAM_POLL')
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self.service_problem('TELEGRAM_POLL', type(exc).__name__)
                await asyncio.sleep(max(10, getattr(exc, 'delay', 10)))

    async def health(self):
        now = self.now()
        if not self.health_url or not self.ready or self.pending_full:
            return
        if not (0 <= now - self.last_stream['mark'] < 15_000 and 0 <= now - self.last_stream['price'] < 120_000):
            return
        if any(key in self.services for key in ('EXCHANGE', 'FUNDING_INFO', 'CLOCK')):
            return
        try:
            async with self.session.get(self.health_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    raise ApiFault(0, 60, 'External health ping failed')
        except (aiohttp.ClientError, asyncio.TimeoutError):
            raise ApiFault(0, 60, 'External health ping unavailable') from None

    async def run(self):
        tasks = [asyncio.create_task(self.db_loop()), asyncio.create_task(self.stream_loop('mark')),
                 asyncio.create_task(self.stream_loop('price')), asyncio.create_task(self.sender_loop()),
                 asyncio.create_task(self.commands_loop()), asyncio.create_task(self.candle_loop())]
        for key, work, seconds in [('CLOCK', self.sync_clock, 600), ('EXCHANGE', self.discover, 86400),
              ('FUNDING_INFO', self.refresh_funding, 10),
              ('FUNDING_HISTORY', self.supplement, 60), ('LIVE_RECHECK', self.reconcile_live, 10),
              ('DIAGNOSTICS', self.diagnostics, 15), ('HEALTHCHECKS', self.health, 60)]:
            tasks.append(asyncio.create_task(self.repeat(key, work, seconds)))
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.store.close()


async def main():
    required = ('DATABASE_URL', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID')
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise SystemExit('Missing required environment variables: ' + ', '.join(missing))
    if not os.environ['DATABASE_URL'].startswith(('postgres://', 'postgresql://')):
        raise SystemExit('DATABASE_URL must point to persistent PostgreSQL')
    maximum = int(os.environ.get('MAX_PENDING_MESSAGES', '2000'))
    if not 100 <= maximum <= 10000:
        raise SystemExit('Safety budgets outside allowed range')
    LOG.info('Starting funding monitor v3')
    store = Store(os.environ['DATABASE_URL'], maximum)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=12),
                                    timeout=aiohttp.ClientTimeout(total=20), trust_env=False) as session:
        await Bot(store, session, os.environ['TELEGRAM_BOT_TOKEN'], os.environ['TELEGRAM_CHAT_ID'],
                  os.environ.get('HEALTHCHECKS_PING_URL', '')).run()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
