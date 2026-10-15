"""Offline runtime integration: mocked HTTP, WebSockets and PostgreSQL."""
import asyncio
import copy
import json
from decimal import Decimal as D
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import app
import api
from engine import DAY_MS, HOUR_MS, evaluate, to_dict
from storage import Backpressure, Store
from test_engine import NOW, ready


class Context:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *args):
        return False


class Response:
    def __init__(self, status=200, body=None, headers=None):
        self.status = status
        self.body = body if body is not None else {}
        self.headers = headers or {}

    async def json(self, **kwargs):
        if isinstance(self.body, Exception):
            raise self.body
        return self.body


class MemoryStore:
    max_pending = 2000

    def __init__(self):
        self.snapshots = {}
        self.pending = {}
        self.meta = {}
        self.commits = []
        self.fail = False

    async def commit(self, states=(), events=(), meta=None, delete=()):
        if self.fail:
            raise OSError('mock database outage')
        self.commits.append((copy.deepcopy(states), list(events), copy.deepcopy(meta)))
        for symbol, payload in states:
            self.snapshots[symbol] = copy.deepcopy(payload)
        for event in events:
            self.pending.setdefault(event.id, {'id': event.id, 'symbol': event.symbol,
                'body': event.message, 'detected_ms': event.detected_ms,
                'attempts': 0, 'due_ms': 0})
        self.meta.update(copy.deepcopy(meta or {}))
        for symbol in delete:
            self.snapshots.pop(symbol, None)

    async def first_pending(self, now):
        available = [row for row in self.pending.values() if row['due_ms'] <= now]
        return copy.deepcopy(min(available, key=lambda row: row['detected_ms'])) if available else None

    async def retry(self, event_id, attempts, due_ms):
        self.pending[event_id].update(attempts=attempts, due_ms=due_ms)

    async def complete(self, event_id):
        del self.pending[event_id]

    async def count_pending(self):
        return len(self.pending)


class Transaction:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        self.before = copy.deepcopy((self.connection.states, self.connection.candles,
                                     self.connection.pending, self.connection.meta))
        self.connection.in_transaction = True
        return self

    async def __aexit__(self, kind, value, trace):
        self.connection.in_transaction = False
        if kind:
            (self.connection.states, self.connection.candles,
             self.connection.pending, self.connection.meta) = self.before
            self.connection.rollbacks += 1
        else:
            self.connection.commits += 1
        return False


class Connection:
    def __init__(self):
        self.states, self.candles, self.pending, self.meta = {}, {}, {}, {}
        self.in_transaction = False
        self.commits = self.rollbacks = 0
        self.fail_on = None
        self.executions = []

    def transaction(self):
        return Transaction(self)

    async def fetchval(self, query, *values):
        if 'count(*)' in query:
            return len(self.pending)
        if "key='cg_budget'" in query:
            return self.meta.get('cg_budget')
        return True

    async def execute(self, query, *values):
        if self.fail_on and self.fail_on in query:
            raise OSError('mock transaction failure')
        self.executions.append((query, values, self.in_transaction))
        if query.startswith('INSERT INTO bot_candles'):
            self.candles[values[0]] = json.loads(values[1])
        elif query.startswith('INSERT INTO bot_symbols'):
            self.states[values[0]] = json.loads(values[1])
        elif query.startswith('INSERT INTO bot_pending'):
            self.pending.setdefault(values[0], {'id': values[0], 'symbol': values[1],
                'body': values[2], 'detected_ms': values[3], 'attempts': 0, 'due_ms': 0})
        elif query.startswith("INSERT INTO bot_meta VALUES('cg_budget'"):
            self.meta['cg_budget'] = json.loads(values[0])
        elif query.startswith('INSERT INTO bot_meta'):
            self.meta[values[0]] = json.loads(values[1])
        elif query.startswith('DELETE FROM bot_symbols'):
            self.states.pop(values[0], None)
        elif query.startswith('DELETE FROM bot_candles'):
            self.candles.pop(values[0], None)
        elif query.startswith('DELETE FROM bot_pending'):
            self.pending.pop(values[0], None)
        elif query.startswith('UPDATE bot_pending'):
            self.pending[values[0]].update(attempts=values[1], due_ms=values[2])
        return 'OK'


def bot_fixture():
    store = MemoryStore()
    session = Mock()
    bot = app.Bot(store, session, 'fake-test-token', '100', 'fake-test-cg-key')
    bot.ready = True
    bot.now = lambda: NOW
    bot.started_ms = NOW - 120_000
    state = ready()
    bot.states[state.symbol] = state
    # Domain rule resolution has separate tests; retain this mocked interval.
    bot.apply_rule = Mock()
    return bot, store, session, state


async def cancel_sleep(*args, **kwargs):
    raise asyncio.CancelledError()


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_late_price_queues_a_once(self):
        bot, store, _, state = bot_fixture()
        state.price, state.price_ms = D('2.99'), NOW - 1
        await bot.assess([state.symbol])
        self.assertFalse(bot.states[state.symbol].a_sent)
        await bot.ingest('price', [{'e': '24hrMiniTicker', 's': state.symbol, 'E': NOW, 'c': '3'}])
        self.assertEqual(len(store.pending), 1)
        self.assertTrue(bot.states[state.symbol].a_sent)
        await bot.ingest('price', [{'s': state.symbol, 'E': NOW, 'c': '3'}])
        self.assertEqual(len(store.pending), 1)

    async def test_stale_empty_and_invalid_stream_rows_do_not_detect(self):
        bot, store, _, state = bot_fixture()
        state.price, state.price_ms = D('2.9'), NOW - 1
        for stream, rows in [('price', []), ('price', [{'s': state.symbol, 'E': NOW - 120_000, 'c': '3'}]),
                             ('mark', [{'s': state.symbol, 'E': NOW - 15_000, 'r': '-0.02', 'T': NOW + 1000}]),
                             ('price', [{'s': state.symbol, 'E': NOW, 'c': 'NaN'}])]:
            with self.assertRaises(api.ApiFault):
                await bot.ingest(stream, rows)
        self.assertEqual(store.pending, {})
        self.assertEqual(bot.last_stream, {'mark': 0, 'price': 0})

    async def test_malformed_next_funding_does_not_refresh_health(self):
        bot, store, _, state = bot_fixture()
        state.funding_ms = NOW - 1
        with self.assertRaises(api.ApiFault):
            await bot.ingest('mark', [{'s': state.symbol, 'E': NOW, 'r': '-0.02', 'T': NOW}])
        self.assertEqual(bot.last_stream['mark'], 0)
        self.assertEqual(store.pending, {})

    async def test_persist_failure_retains_unsent_flags_and_pauses_detection(self):
        bot, store, _, state = bot_fixture()
        store.fail = True
        with self.assertRaises(OSError):
            await bot.assess([state.symbol])
        self.assertFalse(bot.ready)
        self.assertFalse(bot.states[state.symbol].a_sent)
        self.assertIsNone(bot.states[state.symbol].a_epoch)
        self.assertEqual(store.pending, {})
        store.fail = False
        await bot.assess([state.symbol])
        self.assertEqual(store.pending, {})
        bot.ready = True
        await bot.assess([state.symbol])
        self.assertEqual(len(store.pending), 1)

    async def test_funding_rule_freshness_uses_actual_poll_timestamp(self):
        bot, _, _, state = bot_fixture()
        bot.apply_rule = app.Bot.apply_rule.__get__(bot)
        observed_ms = NOW - 240_000
        bot.adjustments_ms = observed_ms
        bot.adjustments = {state.symbol: {'symbol': state.symbol,
            'fundingIntervalHours': 4, 'adjustedFundingRateFloor': '-0.02'}}
        bot.apply_rule(state)
        self.assertEqual(state.interval_hours, 4)
        self.assertEqual(state.interval_ms, observed_ms)
        self.assertEqual(state.floor_ms, observed_ms)
        self.assertEqual(state.last_official_floor_ms, observed_ms)
        # An unrelated current-price refresh cannot re-verify official metadata.
        bot.now = lambda: NOW + 10_000
        bot.apply_rule(state)
        self.assertEqual(state.interval_ms, observed_ms)
        self.assertEqual(state.floor_ms, observed_ms)
        self.assertEqual(state.last_official_floor_ms, observed_ms)
        # Once the official poll expires, no default or interval is invented;
        # the known official floor remains explicitly a bounded fallback.
        bot.now = lambda: NOW + 60_001
        bot.apply_rule(state)
        self.assertIsNone(state.interval_hours)
        self.assertEqual(state.interval_ms, 0)
        self.assertEqual(state.floor_source, 'LAST_VERIFIED_OFFICIAL')
        self.assertEqual(state.floor_ms, observed_ms)
        self.assertEqual(state.last_official_floor_ms, observed_ms)
        self.assertEqual(evaluate(state, bot.now()), [])

    async def test_stale_symbol_funding_rechecked_while_other_ws_active(self):
        bot, store, _, stale = bot_fixture()
        stale.funding_ms = NOW - 15_000
        other = ready()
        other.symbol, other.funding = 'OTHERUSDT', D('0.001')
        bot.states[other.symbol] = other
        bot.last_stream = {'mark': NOW - 1_000, 'price': NOW}
        bot.binance.get = AsyncMock(return_value=[
            {'symbol': stale.symbol, 'time': NOW, 'lastFundingRate': '-0.02', 'nextFundingTime': NOW + HOUR_MS},
            {'symbol': other.symbol, 'time': NOW, 'lastFundingRate': '0.001', 'nextFundingTime': NOW + HOUR_MS}])
        bot.binance.prices = AsyncMock()
        await bot.reconcile_live()
        bot.binance.get.assert_awaited_once_with('/premiumIndex', weight=10)
        bot.binance.prices.assert_not_awaited()
        self.assertEqual(len(store.pending), 1)
        self.assertEqual(next(iter(store.pending.values()))['symbol'], stale.symbol)
        self.assertEqual(bot.states[stale.symbol].funding_ms, NOW)
        self.assertEqual(bot.last_stream['mark'], NOW - 1_000)

    async def test_stale_symbol_rest_response_cannot_create_signal(self):
        bot, store, _, stale = bot_fixture()
        stale.funding_ms = NOW - 15_000
        other = ready()
        other.symbol, other.funding = 'OTHERUSDT', D('0.001')
        bot.states[other.symbol] = other
        bot.last_stream = {'mark': NOW, 'price': NOW}
        bot.binance.get = AsyncMock(return_value=[
            {'symbol': stale.symbol, 'time': NOW - 15_000, 'lastFundingRate': '-0.02', 'nextFundingTime': NOW + HOUR_MS},
            {'symbol': other.symbol, 'time': NOW, 'lastFundingRate': '0.001', 'nextFundingTime': NOW + HOUR_MS}])
        bot.binance.prices = AsyncMock()
        await bot.reconcile_live()
        bot.binance.get.assert_awaited_once_with('/premiumIndex', weight=10)
        self.assertEqual(store.pending, {})
        self.assertEqual(bot.states[stale.symbol].funding_ms, NOW - 15_000)
        self.assertEqual(bot.last_stream['mark'], NOW)

    async def test_stale_symbol_trade_price_uses_batch_actual_price_endpoint(self):
        bot, store, _, stale = bot_fixture()
        stale.price_ms = NOW - 120_000
        other = ready()
        other.symbol, other.funding = 'OTHERUSDT', D('0.001')
        bot.states[other.symbol] = other
        bot.last_stream = {'mark': NOW, 'price': NOW - 1_000}
        bot.binance.get = AsyncMock()
        bot.binance.prices = AsyncMock(return_value=[
            {'symbol': stale.symbol, 'time': NOW, 'price': '3'},
            {'symbol': other.symbol, 'time': NOW, 'price': '3'}])
        await bot.reconcile_live()
        bot.binance.get.assert_not_awaited()
        bot.binance.prices.assert_awaited_once()
        self.assertEqual(len(store.pending), 1)
        self.assertEqual(bot.states[stale.symbol].price_ms, NOW)
        self.assertEqual(bot.last_stream['price'], NOW - 1_000)

    async def test_relisting_requires_verified_time_and_preserves_exact_grace_boundary(self):
        bot, store, _, old = bot_fixture()
        source_onboard = old.onboard_ms
        row = {'symbol': old.symbol, 'baseAsset': old.base, 'quoteAsset': 'USDT',
               'marginAsset': 'USDT', 'contractType': 'PERPETUAL',
               'status': 'TRADING', 'onboardDate': source_onboard}
        bot.meta['initialized'] = True
        bot.rows = {old.symbol: dict(row)}
        bot.binance.get = AsyncMock(return_value={'symbols': [{**row, 'status': 'CLOSE'}]})
        await bot.discover()
        self.assertNotIn(old.symbol, bot.states)
        bot.binance.get.return_value = {'symbols': [row]}
        relisted_ms = NOW + DAY_MS
        bot.now = lambda: relisted_ms
        await bot.discover()
        self.assertEqual(bot.states[old.symbol].onboard_ms, 0)
        self.assertEqual(bot.states[old.symbol].listing_status, 'RELISTING_TIME_UNKNOWN')
        self.assertEqual(bot.states[old.symbol].source_onboard_ms, source_onboard)
        bot.now = lambda: relisted_ms + DAY_MS
        await bot.discover()
        self.assertEqual(bot.states[old.symbol].onboard_ms, 0)
        self.assertEqual(store.pending, {})
        # An actual new official date resolves the hold; no observed date was invented.
        updated = {**row, 'onboardDate': relisted_ms}
        bot.binance.get.return_value = {'symbols': [updated]}
        await bot.discover()
        self.assertEqual(bot.states[old.symbol].onboard_ms, relisted_ms)
        self.assertEqual(bot.states[old.symbol].listing_status, 'CONFIRMED')
        before = relisted_ms + 168 * HOUR_MS - 1
        eligible = ready(now=before, onboard=relisted_ms)
        eligible.source_onboard_ms = relisted_ms
        bot.states[old.symbol] = eligible
        bot.now = lambda: before
        await bot.discover()
        self.assertEqual(store.pending, {})
        from test_engine import fill_history
        exact = before + 1
        fill_history(bot.states[old.symbol], exact)
        bot.now = lambda: exact
        await bot.assess([old.symbol])
        self.assertEqual(len(store.pending), 1)
        self.assertEqual(bot.states[old.symbol].onboard_ms, relisted_ms)

    async def test_full_outbox_prevents_new_detection(self):
        bot, store, _, state = bot_fixture()
        bot.pending_full = True
        await bot.assess([state.symbol])
        self.assertIsNone(state.a_epoch)
        self.assertEqual(store.pending, {})

    async def test_diagnostics_group_once_resolve_then_recur(self):
        bot, store, _, first = bot_fixture()
        second = ready()
        second.symbol = 'OTHERUSDT'
        first.interval_hours = second.interval_hours = None
        bot.states[second.symbol] = second
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)
        body = next(iter(store.pending.values()))['body']
        self.assertIn('ALTUSDT:INTERVAL_UNKNOWN', body)
        self.assertIn('OTHERUSDT:INTERVAL_UNKNOWN', body)
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)
        first.interval_hours = second.interval_hours = 1
        await bot.diagnostics()
        self.assertEqual(bot.meta['diagnostics'], {})
        first.interval_hours = None
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 2)

    async def test_sender_failure_preserves_and_later_sends_ended_signal(self):
        bot, store, _, state = bot_fixture()
        event = evaluate(state, NOW)[0]
        await store.commit([(state.symbol, to_dict(state))], [event])
        bot.telegram.send = AsyncMock(side_effect=api.ApiFault(429, 120))
        with patch('app.asyncio.sleep', side_effect=cancel_sleep):
            with self.assertRaises(asyncio.CancelledError):
                await bot.sender_loop()
        self.assertIn(event.id, store.pending)
        self.assertEqual(store.pending[event.id]['attempts'], 1)
        self.assertEqual(store.pending[event.id]['due_ms'], NOW + 120_000)
        state.funding, state.price = D('0.01'), D('1')
        bot.now = lambda: NOW + 120_001
        bot.telegram.send = AsyncMock()
        with patch('app.asyncio.sleep', side_effect=cancel_sleep):
            with self.assertRaises(asyncio.CancelledError):
                await bot.sender_loop()
        self.assertEqual(store.pending, {})
        delivered = bot.telegram.send.await_args.args[0]
        self.assertIn(event.message, delivered)
        self.assertIn('감지 시각:', delivered)
        self.assertIn('지연 발송:', delivered)

    async def test_commands_only_authorized_chat_status(self):
        bot, store, _, _ = bot_fixture()
        bot.telegram.updates = AsyncMock(side_effect=[[
            {'update_id': 2, 'message': {'chat': {'id': 200}, 'text': '/status'}},
            {'update_id': 3, 'message': {'chat': {'id': 100}, 'text': '/other'}},
            {'update_id': 4, 'message': {'chat': {'id': 100}, 'text': '/status@my_bot'}},
        ], asyncio.CancelledError()])
        bot.telegram.send = AsyncMock()
        bot.status = AsyncMock(return_value='mock status')
        with self.assertRaises(asyncio.CancelledError):
            await bot.commands_loop()
        bot.telegram.send.assert_awaited_once_with('mock status')
        self.assertEqual(store.meta['telegram_offset'], 5)
        self.assertEqual(bot.telegram.updates.await_args_list[-1].args, (5,))

    async def test_websocket_disconnect_reconnects(self):
        bot, _, session, _ = bot_fixture()
        closed = SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data='')
        text = SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data='[]')
        first_ws = SimpleNamespace(receive=AsyncMock(return_value=closed))
        second_ws = SimpleNamespace(receive=AsyncMock(side_effect=[text, asyncio.CancelledError()]))
        session.ws_connect.side_effect = [Context(first_ws), Context(second_ws)]
        bot.ingest = AsyncMock()
        bot.service_problem = AsyncMock()
        with patch('app.asyncio.sleep', new=AsyncMock()) as sleep, patch('app.random.random', return_value=0):
            with self.assertRaises(asyncio.CancelledError):
                await bot.stream_loop('mark')
        self.assertEqual(session.ws_connect.call_count, 2)
        bot.ingest.assert_awaited_once_with('mark', [])
        sleep.assert_awaited_once_with(1)

    async def test_websocket_429_respects_retry_after(self):
        bot, _, session, _ = bot_fixture()
        fault = api.ApiFault(429, 1)
        fault.headers = {'Retry-After': '123'}
        session.ws_connect.side_effect = [fault, asyncio.CancelledError()]
        bot.service_problem = AsyncMock()
        with patch('app.asyncio.sleep', new=AsyncMock()) as sleep, patch('app.random.random', return_value=0):
            with self.assertRaises(asyncio.CancelledError):
                await bot.stream_loop('price')
        sleep.assert_awaited_once_with(123)

    async def test_health_pings_only_with_fresh_real_data(self):
        bot, _, session, _ = bot_fixture()
        bot.health_url = 'https://mock.invalid/private-health'
        session.get.return_value = Context(Response())
        bot.last_stream = {'mark': NOW - 15_000, 'price': NOW}
        await bot.health()
        session.get.assert_not_called()
        bot.last_stream['mark'] = NOW
        await bot.health()
        session.get.assert_called_once()
        session.get.reset_mock()
        bot.last_stream['price'] = NOW - 120_000
        await bot.health()
        session.get.assert_not_called()

    async def test_health_service_fault_or_db_outage_suppresses_ping(self):
        bot, _, session, _ = bot_fixture()
        bot.health_url = 'https://mock.invalid/private-health'
        bot.last_stream = {'mark': NOW, 'price': NOW}
        for service in ('EXCHANGE', 'FUNDING_INFO', 'COINGECKO', 'CLOCK'):
            bot.services = {service: 'mock fault'}
            await bot.health()
        bot.services = {}
        bot.ready = False
        await bot.health()
        session.get.assert_not_called()


class StorageTests(unittest.IsolatedAsyncioTestCase):
    def store(self, maximum=2000):
        store = Store('postgresql://mock.invalid/fake', max_pending=maximum)
        store.conn = Connection()
        return store

    async def test_atomic_event_flag_outbox_and_candles(self):
        store = self.store()
        state = ready()
        event = evaluate(state, NOW)[0]
        await store.commit([(state.symbol, to_dict(state))], [event])
        self.assertTrue(store.conn.states[state.symbol]['a_sent'])
        self.assertIn(event.id, store.conn.pending)
        self.assertEqual(len(store.conn.candles[state.symbol]), len(state.candles))
        self.assertEqual(store.conn.commits, 1)
        self.assertTrue(all(item[2] for item in store.conn.executions))
        self.assertEqual(store.history_revisions[state.symbol], state.history_revision)

    async def test_transaction_rollback_restores_state_and_revision(self):
        store = self.store()
        state = ready()
        event = evaluate(state, NOW)[0]
        store.conn.fail_on = 'INSERT INTO bot_pending'
        with self.assertRaises(OSError):
            await store.commit([(state.symbol, to_dict(state))], [event])
        self.assertEqual(store.conn.states, {})
        self.assertEqual(store.conn.candles, {})
        self.assertEqual(store.conn.pending, {})
        self.assertEqual(store.history_revisions, {})
        self.assertEqual(store.conn.rollbacks, 1)

    async def test_storage_updates_candles_only_when_revision_changes(self):
        store = self.store()
        state = ready()
        await store.commit([(state.symbol, to_dict(state))])
        initial = sum('INSERT INTO bot_candles' in q for q, _, _ in store.conn.executions)
        snapshot = to_dict(state)
        snapshot.pop('candles')
        await store.commit([(state.symbol, snapshot)])
        self.assertEqual(sum('INSERT INTO bot_candles' in q for q, _, _ in store.conn.executions), initial)
        state.history_revision += 1
        await store.commit([(state.symbol, to_dict(state))])
        self.assertEqual(sum('INSERT INTO bot_candles' in q for q, _, _ in store.conn.executions), initial + 1)

    async def test_delisting_keeps_already_detected_pending_signal(self):
        store = self.store()
        state = ready()
        event = evaluate(state, NOW)[0]
        await store.commit([(state.symbol, to_dict(state))], [event])
        await store.commit(delete=[state.symbol])
        self.assertEqual(store.conn.states, {})
        self.assertEqual(store.conn.candles, {})
        self.assertIn(event.id, store.conn.pending)

    async def test_outbox_bound_rejects_detection_atomically(self):
        store = self.store(maximum=1)
        state = ready()
        first = evaluate(state, NOW)[0]
        await store.commit([(state.symbol, to_dict(state))], [first])
        second_state = ready()
        second = evaluate(second_state, NOW)[0]
        with self.assertRaises(Backpressure):
            await store.commit([(second_state.symbol, to_dict(second_state))], [second])
        self.assertEqual(set(store.conn.pending), {first.id})
        self.assertEqual(store.conn.states[state.symbol]['a_epoch'], state.a_epoch)

    async def test_rolling_cg_attempt_budget_persisted_and_bounded(self):
        store = self.store()
        self.assertEqual(await store.reserve_cg_call(NOW, 2), 1)
        self.assertEqual(await store.reserve_cg_call(NOW, 2), 2)
        with self.assertRaises(Backpressure):
            await store.reserve_cg_call(NOW, 2)
        self.assertEqual(sum(store.conn.meta['cg_budget']['buckets'].values()), 2)
        with self.assertRaises(Backpressure):
            await store.reserve_cg_call(NOW + 31 * DAY_MS, 2)
        self.assertEqual(await store.reserve_cg_call(NOW + 32 * DAY_MS, 2), 1)
        self.assertLessEqual(len(store.conn.meta['cg_budget']['buckets']), 32)


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_after_header_and_blocked_statuses(self):
        for status, headers, delay in [(429, {'Retry-After': '123'}, 123),
                                        (403, {}, 1800), (451, {}, 1800)]:
            session = Mock()
            session.request.return_value = Context(Response(status, headers=headers))
            with self.assertRaises(api.ApiFault) as result:
                await api.json_request(session, 'GET', 'https://mock.invalid/no-network')
            self.assertEqual(result.exception.delay, delay)
            self.assertEqual(result.exception.status, status)

    async def test_telegram_429_body_retry_after(self):
        session = Mock()
        session.request.return_value = Context(Response(429, {'ok': False,
            'error_code': 429, 'parameters': {'retry_after': 123}}))
        telegram = api.Telegram(session, 'fake-test-token', '100')
        with self.assertRaises(api.ApiFault) as result:
            await telegram.send('offline mock')
        self.assertGreaterEqual(result.exception.delay, 123)

    async def test_rate_budget_waits_and_releases_after_window(self):
        clock = [0.0]
        waits = []
        async def advance(seconds):
            waits.append(seconds)
            clock[0] += seconds
        budget = api.WindowBudget(2, 60, clock=lambda: clock[0], sleep=advance)
        await budget.take(2)
        await budget.take()
        self.assertEqual(waits, [60])
        self.assertEqual(sum(weight for _, weight in budget.items), 1)
        with self.assertRaises(ValueError):
            await budget.take(3)

    async def test_cooldown_started_while_budget_waiting_delays_http(self):
        client = api.Binance(Mock())
        clock = [0.0]
        waits = []
        async def budget_wait(*args):
            client.blocked_until = 20.0
        async def advance(seconds):
            waits.append(seconds)
            clock[0] += seconds
        client.weights.take = AsyncMock(side_effect=budget_wait)
        client.funding.take = AsyncMock()
        async def respond(*args, **kwargs):
            self.assertGreaterEqual(clock[0], 20.0)
            return [], 0
        with patch('api.time.monotonic', side_effect=lambda: clock[0]), \
             patch('api.asyncio.sleep', side_effect=advance), \
             patch('api.json_request', side_effect=respond):
            self.assertEqual(await client.get('/fundingInfo'), [])
        self.assertEqual(waits, [20.0])

    async def test_source_onboard_metadata_serializes_without_event_reset(self):
        from engine import from_dict
        state = ready()
        state.source_onboard_ms = NOW - 100 * DAY_MS
        first = evaluate(state, NOW)[0]
        restored = from_dict(json.loads(json.dumps(to_dict(state))))
        self.assertEqual(restored.source_onboard_ms, state.source_onboard_ms)
        self.assertEqual(evaluate(restored, NOW), [])
        self.assertTrue(first.id.endswith(restored.a_epoch))

    async def test_malformed_http_json_and_network_error_are_safe(self):
        for failure in (ValueError('malformed'), aiohttp.ClientError('secret URL')):
            session = Mock()
            session.request.return_value = Context(Response(body=failure))
            with self.assertRaises(api.ApiFault) as result:
                await api.json_request(session, 'GET', 'https://mock.invalid/no-network')
            self.assertNotIn('secret', str(result.exception))


if __name__ == '__main__':
    unittest.main()


