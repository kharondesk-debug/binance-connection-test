"""Offline approval, runtime freshness, persistence and startup regressions."""
import asyncio
import copy
import json
from decimal import Decimal as D
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, Mock, patch
import unittest

import app
from api import ApiFault
from engine import DAY_MS, HOUR_MS, State, from_dict, to_dict
from policy import load_seed
from storage import PolicyConflict
from test_engine import NOW, fill_history, ready


def exchange_row(symbol='NEWV4USDT', onboard=None, status='TRADING'):
    return {'symbol': symbol, 'baseAsset': symbol[:-4], 'quoteAsset': 'USDT',
            'marginAsset': 'USDT', 'contractType': 'PERPETUAL', 'status': status,
            'onboardDate': NOW - 30*DAY_MS if onboard is None else onboard}


class MemoryStore:
    """Transactional fake, preserving archived candle rows when caches are dropped."""
    max_pending = 2000
    def __init__(self):
        self.states, self.histories, self.pending, self.meta, self.watch = {}, {}, {}, {}, {}
        self.commits, self.history_reads = [], []
        self.fail = False
    async def commit(self, states=(), events=(), meta=None, delete=(), watch=()):
        states, events, watch = list(states), list(events), list(watch)
        if self.fail:
            raise OSError('offline database failure')
        for record in watch:
            current = self.watch.get(record['symbol'])
            if current and (record.get('expected_generation') != current['generation'] or
                    (record.get('expected_status') is not None and record['expected_status'] != current['status'])):
                raise PolicyConflict('offline compare-and-set conflict')
        self.commits.append({'states': copy.deepcopy(states), 'events': list(events),
                             'meta': copy.deepcopy(meta or {}), 'watch': copy.deepcopy(watch), 'delete': list(delete)})
        for symbol, payload in states:
            payload = copy.deepcopy(payload)
            if 'candles' in payload:
                self.histories[symbol] = payload.pop('candles')
            self.states[symbol] = payload
        for record in watch:
            self.watch[record['symbol']] = app.Bot.clean_watch(copy.deepcopy(record))
        for event in events:
            self.pending.setdefault(event.id, {'id': event.id, 'symbol': event.symbol,
                'body': event.message, 'detected_ms': event.detected_ms,
                'reply_markup': event.reply_markup, 'attempts': 0, 'due_ms': 0})
        self.meta.update(copy.deepcopy(meta or {}))
        for symbol in delete:
            self.states.pop(symbol, None)
            self.histories.pop(symbol, None)
    async def load_history(self, symbol):
        self.history_reads.append(symbol)
        return from_dict({'symbol': symbol, 'base': symbol[:-4], 'onboard_ms': 1,
                          'candles': copy.deepcopy(self.histories.get(symbol, []))}).candles
    async def count_pending(self):
        return len(self.pending)
    async def load_daily(self):
        return copy.deepcopy(self.meta.get('daily_report_state'))
    async def save_daily(self, snapshot, completed=()):
        if self.fail:
            raise OSError('offline database failure')
        self.meta['daily_report_state'] = copy.deepcopy(snapshot)
    async def migrate_daily_diagnostics(self, now):
        return {'count': 0, 'first_ms': None, 'last_ms': None}
    async def legacy_summary(self):
        return {'count': 0, 'first_ms': None, 'last_ms': None}
    async def first_daily(self, now):
        return None
    async def first_pending(self, now):
        rows = [r for r in self.pending.values() if r['due_ms'] <= now]
        return copy.deepcopy(min(rows, key=lambda r: (r['detected_ms'], r['id']))) if rows else None
    async def complete(self, event_id, message_id=None):
        for record in self.watch.values():
            if record.get('notice_id') == event_id:
                record['message_id'] = message_id
        self.pending.pop(event_id, None)
    async def retry(self, event_id, attempts, due_ms):
        self.pending[event_id].update(attempts=attempts, due_ms=due_ms)
    async def close(self):
        pass
    async def open(self):
        pass
    async def seed_policy(self, seed):
        return False
    async def load_policy(self):
        return copy.deepcopy(self.watch)
    async def load_states(self, approved):
        self.selected_approved = set(approved)
        states = {}
        for symbol, data in self.states.items():
            data = copy.deepcopy(data)
            data['candles'] = copy.deepcopy(self.histories.get(symbol, [])) if symbol in approved else []
            states[symbol] = from_dict(data)
        return states, copy.deepcopy(self.meta)


def fixture(interval=1, status='APPROVED', chat_id='100', admin=None):
    store = MemoryStore()
    bot = app.Bot(store, Mock(), 'offline-fake-token', chat_id, admin_user_id=admin)
    for group, policy in (('approved', 'APPROVED'), ('blocked', 'BLOCKED'), ('review', 'REVIEW')):
        for symbol in bot.seed[group]:
            bot.watch[symbol] = {'symbol': symbol, 'status': policy, 'generation': 1,
                'listing_key': '', 'notice_id': '', 'message_id': None}
    state = ready(interval)
    state.source_onboard_ms = state.onboard_ms
    bot.states[state.symbol] = state
    bot.watch[state.symbol].update(status=status, listing_key=str(state.onboard_ms))
    if status in ('PENDING', 'REVIEW'):
        bot.watch[state.symbol]['notice_id'] = f'notice:{state.symbol}:1'
    store.watch = copy.deepcopy(bot.watch)
    store.histories[state.symbol] = to_dict(state)['candles']
    bot.ready = True
    bot.now = lambda: NOW
    bot.started_ms = NOW - 120_000
    bot.apply_rule = Mock()
    bot.telegram.send = AsyncMock(return_value={'message_id': 500})
    bot.telegram.edit = AsyncMock(return_value=True)
    bot.telegram.answer_callback = AsyncMock(return_value=True)
    bot.binance.get = AsyncMock()
    bot.binance.prices = AsyncMock()
    return bot, store, state


def callback(symbol='ALTUSDT', generation=1, action='a', user=100, chat=100):
    return {'update_id': 11, 'callback_query': {'id': 'query-11', 'from': {'id': user},
        'message': {'chat': {'id': chat}, 'message_id': 500},
        'data': f'watch:{action}:{generation}:{symbol}'}}


def command(text, user=100, chat=100):
    return {'update_id': 12, 'message': {'chat': {'id': chat}, 'from': {'id': user}, 'text': text}}


class ContractTests(TestCase):
    def test_exact_four_binance_metadata_filters(self):
        row = exchange_row('1000CATUSDT')
        self.assertTrue(app.accepted_contract(row))
        for key, value in (('status', 'BREAK'), ('contractType', 'CURRENT_QUARTER'),
                           ('marginAsset', 'USDC'), ('quoteAsset', 'USDC')):
            self.assertFalse(app.accepted_contract({**row, key: value}))
        self.assertFalse(app.accepted_contract(None))
    def test_scheduling_constants(self):
        self.assertEqual(app.REGULAR_REST_SECONDS, 180)
        self.assertEqual(app.DIAGNOSTICS_SECONDS, 15)
        self.assertLessEqual(app.RECOVERY_MIN_SECONDS, 15)


class ApprovalRuntimeTests(IsolatedAsyncioTestCase):
    async def test_live_price_and_mark_keep_immediate_a_evaluation(self):
        bot, store, state = fixture()
        state.price = D('2.999')
        state.price_ms = NOW-1
        await bot.assess([state.symbol])
        await bot.ingest('price', [{'e': '24hrMiniTicker', 's': state.symbol, 'E': NOW, 'c': '3'}])
        self.assertEqual(len(store.pending), 1)
        self.assertTrue(bot.states[state.symbol].a_sent)
        await bot.ingest('mark', [{'e': 'markPriceUpdate', 's': state.symbol, 'E': NOW,
                                   'r': '-0.02', 'p': '99999', 'T': NOW+HOUR_MS}])
        self.assertEqual(bot.states[state.symbol].price, D('3'))
        self.assertEqual(len(store.pending), 1)

    async def test_nonapproved_states_never_enter_a_or_b_evaluation(self):
        for status in ('BLOCKED', 'PENDING', 'REVIEW'):
            for interval in (1, 4, 8):
                with self.subTest(status=status, interval=interval):
                    bot, store, state = fixture(interval, status)
                    await bot.assess([state.symbol])
                    await bot.ingest('price', [{'s': state.symbol, 'E': NOW, 'c': '3'}])
                    await bot.ingest('mark', [{'s': state.symbol, 'E': NOW, 'r': '-0.02', 'T': NOW+HOUR_MS}])
                    self.assertFalse(store.pending)
                    self.assertIsNone(bot.states[state.symbol].a_epoch)
                    self.assertIsNone(bot.states[state.symbol].b_epoch)

    async def test_new_contract_pending_notice_atomic_and_idempotent(self):
        bot, store, _ = fixture()
        bot.states.clear()
        row = exchange_row()
        bot.binance.get.return_value = {'symbols': [row]}
        await bot.discover()
        record = bot.watch[row['symbol']]
        self.assertEqual(record['status'], 'PENDING')
        self.assertEqual(len(store.pending), 1)
        event_id = record['notice_id']
        self.assertEqual(store.pending[event_id]['reply_markup'], bot.buttons(row['symbol'], record['generation']))
        transaction = next(c for c in store.commits if c['watch'])
        self.assertTrue(transaction['states'])
        self.assertEqual(transaction['events'][0].id, event_id)
        await bot.discover()
        self.assertEqual(len(store.pending), 1)
        self.assertEqual(bot.watch[row['symbol']]['notice_id'], event_id)
        self.assertFalse(bot.states[row['symbol']].candles)

    async def test_old_unknown_contract_startup_does_not_announce_new_listing(self):
        bot, store, _ = fixture()
        bot.states.clear()
        rows = [exchange_row(f'OLDC{index:03}USDT', bot.seed['snapshot_ms']-DAY_MS) for index in range(300)]
        bot.binance.get.return_value = {'symbols': rows}
        await bot.discover()
        self.assertEqual(sum(bot.watch[r['symbol']]['status'] == 'PENDING' for r in rows), 300)
        self.assertFalse(store.pending)
        self.assertTrue(all(not bot.states[r['symbol']].candles for r in rows))

    async def test_initial_525_snapshot_does_not_send_approval_notices(self):
        bot, store, _ = fixture()
        bot.states.clear()
        bot.binance.get.return_value = {'symbols': [exchange_row(s, NOW-30*DAY_MS) for s in bot.seed['snapshot']]}
        await bot.discover()
        self.assertEqual(len(bot.states), 525)
        self.assertFalse(store.pending)
        self.assertEqual(sum(bot.approved(s) for s in bot.states), 337)

    async def test_newly_discovered_30_day_listing_keeps_actual_onboard(self):
        bot, store, _ = fixture()
        bot.states.clear()
        row = exchange_row('AUSDT')
        bot.binance.get.return_value = {'symbols': [row]}
        await bot.discover()
        self.assertEqual(bot.states['AUSDT'].onboard_ms, NOW-30*DAY_MS)
        self.assertTrue(bot.approved('AUSDT'))
        candidate = ready(onboard=NOW-30*DAY_MS)
        candidate.symbol, candidate.base = 'AUSDT', 'A'
        bot.states['AUSDT'] = candidate
        await bot.assess(['AUSDT'])
        self.assertEqual(len(store.pending), 1)

    async def test_relisting_unknown_becomes_pending_without_inventing_time(self):
        bot, store, state = fixture()
        row = exchange_row(state.symbol, state.onboard_ms)
        bot.rows = {state.symbol: row}
        bot.binance.get.return_value = {'symbols': [{**row, 'status': 'CLOSE'}]}
        await bot.discover()
        bot.now = lambda: NOW+DAY_MS
        bot.binance.get.return_value = {'symbols': [row]}
        await bot.discover()
        self.assertEqual(bot.states[state.symbol].onboard_ms, 0)
        self.assertEqual(bot.states[state.symbol].listing_status, 'RELISTING_TIME_UNKNOWN')
        self.assertEqual(bot.watch[state.symbol]['status'], 'PENDING')
        self.assertFalse(bot.states[state.symbol].candles)

    async def test_blocked_relisting_does_not_release_block(self):
        bot, store, state = fixture(status='BLOCKED')
        row = exchange_row(state.symbol, state.onboard_ms)
        bot.rows = {state.symbol: row}
        bot.binance.get.return_value = {'symbols': [{**row, 'status': 'CLOSE'}]}
        await bot.discover()
        bot.now = lambda: NOW+DAY_MS
        bot.binance.get.return_value = {'symbols': [row]}
        await bot.discover()
        self.assertEqual(bot.watch[state.symbol]['status'], 'BLOCKED')
        self.assertFalse(store.pending)

    async def test_confirmed_relisting_uses_new_exchange_time_and_requires_approval(self):
        bot, store, state = fixture()
        row = exchange_row(state.symbol, state.onboard_ms)
        bot.rows = {state.symbol: row}
        bot.binance.get.return_value = {'symbols': [{**row, 'status': 'CLOSE'}]}
        await bot.discover()
        bot.now = lambda: NOW+DAY_MS
        bot.binance.get.return_value = {'symbols': [{**row, 'onboardDate': NOW+DAY_MS-1}]}
        await bot.discover()
        self.assertEqual(bot.states[state.symbol].onboard_ms, NOW+DAY_MS-1)
        self.assertEqual(bot.watch[state.symbol]['status'], 'PENDING')
        self.assertFalse(bot.states[state.symbol].history_ready)

    async def test_block_drops_runtime_caches_keeps_archived_history_and_flags(self):
        bot, store, state = fixture(4)
        await bot.assess([state.symbol])
        state = bot.states[state.symbol]
        epoch = state.b_epoch
        history = copy.deepcopy(store.histories[state.symbol])
        bot.histories[state.symbol] = [{'fundingRate': '-0.02'}]
        bot.history_checked[state.symbol] = NOW
        bot.candle_retry[state.symbol] = NOW
        result = await bot.change_approval('ALT', 'BLOCKED')
        self.assertTrue(result['changed'])
        self.assertFalse(bot.states[state.symbol].candles)
        self.assertIsNone(bot.states[state.symbol].price)
        self.assertNotIn(state.symbol, bot.histories)
        self.assertNotIn(state.symbol, bot.history_checked)
        self.assertNotIn(state.symbol, bot.candle_retry)
        self.assertEqual(store.histories[state.symbol], history)
        self.assertEqual(bot.states[state.symbol].b_epoch, epoch)
        self.assertTrue(bot.states[state.symbol].b_sent)
        self.assertFalse(any(c['delete'] for c in store.commits))

    async def test_approve_reuses_existing_db_history_without_api_call(self):
        bot, store, state = fixture(status='PENDING')
        state.candles = []
        state.history_ready = False
        await bot.change_approval(state.symbol, 'APPROVED')
        self.assertEqual(store.history_reads, [state.symbol])
        self.assertEqual(len(bot.states[state.symbol].candles), len(store.histories[state.symbol]))
        bot.binance.get.assert_not_awaited()
        self.assertIsNone(bot.states[state.symbol].price)
        self.assertIsNone(bot.states[state.symbol].funding)

    async def test_reapproval_does_not_rearm_already_sent_a(self):
        bot, store, state = fixture()
        await bot.assess([state.symbol])
        epoch = bot.states[state.symbol].a_epoch
        await bot.change_approval(state.symbol, 'BLOCKED')
        await bot.change_approval(state.symbol, 'APPROVED')
        current = bot.states[state.symbol]
        current.price, current.funding = D('3'), D('-0.02')
        current.price_ms = current.funding_ms = NOW
        await bot.assess([state.symbol])
        self.assertEqual(current.a_epoch, epoch)
        self.assertEqual(len(store.pending), 1)

    async def test_approval_failure_keeps_durable_status_and_never_reports_success(self):
        bot, store, state = fixture(status='PENDING')
        store.fail = True
        with self.assertRaises(OSError):
            await bot.handle_update(callback())
        self.assertEqual(bot.watch[state.symbol]['status'], 'PENDING')
        self.assertEqual(store.watch[state.symbol]['status'], 'PENDING')
        self.assertFalse(bot.ready)
        self.assertIn('저장 실패', bot.telegram.answer_callback.await_args.args[1])
        bot.telegram.edit.assert_not_awaited()

    async def test_callback_and_command_share_durable_policy(self):
        bot, store, state = fixture(status='PENDING')
        await bot.handle_update(callback())
        self.assertEqual(store.watch[state.symbol]['status'], 'APPROVED')
        await bot.handle_update(command('/block ALT'))
        self.assertEqual(bot.watch[state.symbol]['status'], 'BLOCKED')
        self.assertEqual(store.watch[state.symbol]['status'], 'BLOCKED')
        await bot.handle_update(command('/review ALT'))
        self.assertEqual(bot.watch[state.symbol]['status'], 'REVIEW')

    async def test_duplicate_callback_is_idempotent(self):
        bot, store, state = fixture(status='PENDING')
        await bot.handle_update(callback())
        generation = bot.watch[state.symbol]['generation']
        writes = len(store.commits)
        await bot.handle_update(callback())
        self.assertEqual(bot.watch[state.symbol]['generation'], generation)
        self.assertEqual(len(store.commits), writes)
        self.assertIn('이미 처리된', bot.telegram.answer_callback.await_args.args[1])

    async def test_nonadmin_or_other_chat_cannot_approve_callbacks(self):
        for user, chat in ((101, 100), (100, 101), (101, 101)):
            bot, store, state = fixture(status='PENDING')
            await bot.handle_update(callback(user=user, chat=chat))
            self.assertEqual(bot.watch[state.symbol]['status'], 'PENDING')
            self.assertFalse(store.commits)
            self.assertTrue(bot.telegram.answer_callback.await_args.kwargs['alert'])

    async def test_nonadmin_command_cannot_change_state(self):
        bot, store, state = fixture(status='PENDING')
        await bot.handle_update(command('/approve ALT', user=101))
        self.assertEqual(bot.watch[state.symbol]['status'], 'PENDING')
        self.assertFalse(store.commits)

    async def test_group_without_explicit_admin_safely_rejects(self):
        bot, store, state = fixture(status='PENDING', chat_id='-100')
        await bot.handle_update(command('/approve ALT', user=100, chat=-100))
        self.assertIsNone(bot.admin_id)
        self.assertEqual(bot.watch[state.symbol]['status'], 'PENDING')
        self.assertIn('TELEGRAM_ADMIN_USER_ID', bot.telegram.send.await_args.args[0])

    async def test_group_configured_admin_accepts_only_exact_user_and_chat(self):
        bot, store, state = fixture(status='PENDING', chat_id='-100', admin='123')
        await bot.handle_update(callback(user=100, chat=-100))
        self.assertFalse(store.commits)
        await bot.handle_update(callback(user=123, chat=-100))
        self.assertEqual(bot.watch[state.symbol]['status'], 'APPROVED')

    async def test_malformed_unknown_and_stale_callback_cannot_change_policy(self):
        data = ('watch:x:1:ALTUSDT', 'watch:a:no:ALTUSDT', 'watch:a:1:NOTLISTEDUSDT',
                'watch:a:2:ALTUSDT', 'watch:a:1:ALTUSDT:extra')
        for value in data:
            bot, store, state = fixture(status='PENDING')
            update = callback()
            update['callback_query']['data'] = value
            await bot.handle_update(update)
            self.assertEqual(bot.watch[state.symbol]['status'], 'PENDING')
            self.assertFalse(store.commits)

    async def test_missing_notice_prevents_forged_initial_seed_callback(self):
        bot, store, state = fixture(status='BLOCKED')
        await bot.handle_update(callback())
        self.assertEqual(bot.watch[state.symbol]['status'], 'BLOCKED')
        self.assertFalse(store.commits)

    async def test_failed_edit_does_not_rollback_committed_approval(self):
        bot, store, state = fixture(status='PENDING')
        bot.telegram.edit.side_effect = ApiFault(400, 60, 'offline edit failure')
        await bot.handle_update(callback())
        self.assertEqual(store.watch[state.symbol]['status'], 'APPROVED')
        self.assertEqual(bot.watch[state.symbol]['status'], 'APPROVED')

    async def test_restart_keeps_policy_and_queued_signal_without_duplicate(self):
        bot, store, state = fixture()
        await bot.assess([state.symbol])
        await bot.change_approval(state.symbol, 'BLOCKED')
        restarted = app.Bot(store, Mock(), 'offline-token', '100')
        restarted.now = lambda: NOW
        await restarted.connect_db()
        self.assertFalse(restarted.approved(state.symbol))
        self.assertFalse(restarted.states[state.symbol].candles)
        self.assertTrue(restarted.states[state.symbol].a_sent)
        self.assertEqual(len(store.pending), 1)
        self.assertNotIn(state.symbol, store.selected_approved)

    async def test_unchanged_refresh_does_not_save_all_337_states(self):
        bot, store, _ = fixture()
        bot.states.clear()
        bot.apply_rule = app.Bot.apply_rule.__get__(bot)
        rows = []
        for symbol in bot.seed['approved']:
            state = ready(4)
            state.symbol, state.base = symbol, symbol[:-4]
            state.price = D('2')
            state.next_times = [state.next_funding_ms]
            bot.states[symbol] = state
            rows.append({'symbol': symbol, 'fundingIntervalHours': 4, 'adjustedFundingRateFloor': '-0.02'})
        bot.binance.get.return_value = rows
        await bot.refresh_funding()
        store.commits.clear()
        bot.now = lambda: NOW+180_000
        await bot.refresh_funding()
        self.assertEqual(sum(len(c['states']) for c in store.commits), 0)
        self.assertEqual(sum(len(c['events']) for c in store.commits), 0)
        self.assertLessEqual(sum(len(c['meta']) for c in store.commits), 1)

    async def test_live_values_change_in_memory_without_symbol_saves(self):
        bot, store, state = fixture()
        state.price = D('2')
        state.funding = D('0.001')
        state.next_times = [state.next_funding_ms]
        await bot.assess([state.symbol])
        store.commits.clear()
        bot.now = lambda: NOW+1
        await bot.ingest('price', [{'s': state.symbol, 'E': NOW+1, 'c': '2.1'}])
        await bot.ingest('mark', [{'s': state.symbol, 'E': NOW+1, 'r': '0.002', 'T': NOW+HOUR_MS}])
        self.assertEqual(bot.states[state.symbol].price, D('2.1'))
        self.assertEqual(bot.states[state.symbol].funding, D('0.002'))
        self.assertEqual(sum(len(c['states']) for c in store.commits), 0)

    async def test_pending_list_pages_are_sorted_and_telegram_sized(self):
        bot, store, _ = fixture()
        for i in range(300):
            symbol = f'NEW{i:03}USDT'
            bot.watch[symbol] = {'symbol': symbol, 'status': 'PENDING', 'generation': 1}
        first = bot.pending_page('1')
        second = bot.pending_page('2')
        self.assertLess(len(first), 3801)
        self.assertLess(len(second), 3801)
        self.assertIn('NEW000USDT', first)
        self.assertNotIn('NEW020USDT', first)
        self.assertIn('NEW020USDT', second)

    async def test_status_contains_policy_counts_and_per_symbol_state(self):
        bot, store, state = fixture()
        text = await bot.status()
        self.assertIn('승인: 337', text)
        self.assertIn('차단: 187', text)
        self.assertIn('보류: 1', text)
        self.assertIn('대기: 0', text)
        self.assertIn('승인 상태: APPROVED', await bot.status(state.symbol))

    async def test_stale_data_recovers_without_waiting_180_seconds(self):
        bot, store, state = fixture()
        state.funding_ms = NOW-15_000
        bot.reconcile_live = AsyncMock()
        bot.service_problem = AsyncMock()
        await bot.recovery_once()
        bot.reconcile_live.assert_awaited_once()
        await bot.recovery_once()
        bot.reconcile_live.assert_awaited_once()
        bot.now = lambda: NOW+10_000
        await bot.recovery_once()
        self.assertEqual(bot.reconcile_live.await_count, 2)

    async def test_live_recovery_respects_http_backoff(self):
        bot, store, state = fixture()
        state.funding_ms = NOW-15_000
        bot.reconcile_live = AsyncMock(side_effect=ApiFault(429, 120, 'offline rate limit'))
        bot.service_problem = AsyncMock()
        await bot.recovery_once()
        self.assertGreaterEqual(bot.recovery_due_ms, NOW+120_000)
        bot.now = lambda: NOW+119_999
        await bot.recovery_once()
        bot.reconcile_live.assert_awaited_once()

    async def test_nonapproved_stale_states_do_not_trigger_rest_recovery(self):
        bot, store, state = fixture(status='BLOCKED')
        state.price_ms = state.funding_ms = 0
        bot.reconcile_live = AsyncMock()
        await bot.recovery_once()
        bot.reconcile_live.assert_not_awaited()

    async def test_failed_signal_transaction_does_not_mark_live_state_sent(self):
        bot, store, state = fixture()
        store.fail = True
        with self.assertRaises(OSError):
            await bot.assess([state.symbol])
        self.assertFalse(bot.states[state.symbol].a_sent)
        self.assertIsNone(bot.states[state.symbol].a_epoch)
        self.assertFalse(store.pending)
        self.assertFalse(bot.ready)

    async def test_no_approval_means_no_history_fetch(self):
        bot, store, state = fixture(status='BLOCKED')
        await bot.fetch_candles(state)
        bot.binance.get.assert_not_awaited()


    async def test_relisting_time_confirmation_keeps_original_notice_and_buttons(self):
        bot, store, state = fixture()
        row = exchange_row(state.symbol, state.onboard_ms)
        bot.rows = {state.symbol: row}
        bot.binance.get.return_value = {'symbols': [{**row, 'status': 'CLOSE'}]}
        await bot.discover()
        bot.now = lambda: NOW+DAY_MS
        bot.binance.get.return_value = {'symbols': [row]}
        await bot.discover()
        original = dict(bot.watch[state.symbol])
        count = len(store.pending)
        self.assertTrue(original['notice_id'].startswith('notice:'))
        bot.now = lambda: NOW+2*DAY_MS
        confirmed = NOW+DAY_MS-1
        bot.binance.get.return_value = {'symbols': [{**row, 'onboardDate': confirmed}]}
        await bot.discover()
        self.assertEqual(bot.states[state.symbol].onboard_ms, confirmed)
        self.assertEqual(bot.watch[state.symbol]['notice_id'], original['notice_id'])
        self.assertEqual(bot.watch[state.symbol]['generation'], original['generation'])
        self.assertEqual(len(store.pending), count)
        await bot.handle_update(callback(generation=original['generation']))
        self.assertEqual(bot.watch[state.symbol]['status'], 'APPROVED')

    async def test_unknown_new_onboard_later_confirmed_sends_first_notice_once(self):
        bot, store, _ = fixture()
        bot.states.clear()
        row = exchange_row(onboard=0)
        bot.binance.get.return_value = {'symbols': [row]}
        await bot.discover()
        self.assertFalse(store.pending)
        self.assertEqual(bot.watch[row['symbol']]['status'], 'PENDING')
        row['onboardDate'] = NOW-DAY_MS
        bot.binance.get.return_value = {'symbols': [row]}
        await bot.discover()
        self.assertEqual(len(store.pending), 1)
        record = dict(bot.watch[row['symbol']])
        self.assertTrue(record['notice_id'].startswith('notice:'))
        await bot.discover()
        self.assertEqual(len(store.pending), 1)
        self.assertEqual(bot.watch[row['symbol']]['generation'], record['generation'])

    async def test_failed_approval_does_not_remove_runtime_caches(self):
        bot, store, state = fixture()
        bot.histories[state.symbol] = [{'fundingTime': NOW, 'fundingRate': '-0.02'}]
        bot.history_checked[state.symbol] = NOW
        bot.candle_retry[state.symbol] = NOW+60_000
        before = copy.deepcopy((bot.histories, bot.history_checked, bot.candle_retry))
        store.fail = True
        with self.assertRaises(OSError):
            await bot.change_approval(state.symbol, 'BLOCKED')
        self.assertEqual((bot.histories, bot.history_checked, bot.candle_retry), before)
        self.assertTrue(bot.states[state.symbol].candles)
        self.assertTrue(bot.approved(state.symbol))

    async def test_reapproval_does_not_rearm_already_sent_b(self):
        bot, store, state = fixture(4)
        await bot.assess([state.symbol])
        epoch = bot.states[state.symbol].b_epoch
        await bot.change_approval(state.symbol, 'BLOCKED')
        await bot.change_approval(state.symbol, 'APPROVED')
        current = bot.states[state.symbol]
        current.price, current.funding = D('3'), D('-0.02')
        current.price_ms = current.funding_ms = NOW
        await bot.assess([state.symbol])
        self.assertEqual(bot.states[state.symbol].b_epoch, epoch)
        self.assertEqual(len(store.pending), 1)

    async def test_complete_cached_history_requires_no_redundant_klines_request(self):
        bot, store, state = fixture()
        state.price = D('2')
        await bot.save([state], candles=True)
        await bot.assess([state.symbol])
        state = bot.states[state.symbol]
        store.commits.clear()
        await bot.fetch_candles(state)
        bot.binance.get.assert_not_awaited()
        self.assertEqual(sum(len(c['states']) for c in store.commits), 0)
        self.assertEqual(len(store.histories[state.symbol]), len(state.candles))

    async def test_approval_reuses_history_but_missing_gap_is_fetched_only_once(self):
        bot, store, state = fixture(status='PENDING')
        missing = store.histories[state.symbol].pop(100)
        state.candles = []
        await bot.change_approval(state.symbol, 'APPROVED')
        current = bot.states[state.symbol]
        bot.binance.get.return_value = [[missing['open_ms'], '0','0','0', missing['close'], '0', missing['close_ms']]]
        await bot.fetch_candles(current)
        call = bot.binance.get.await_args
        self.assertEqual(call.args[0], '/klines')
        self.assertEqual(call.args[1]['startTime'], missing['open_ms'])
        self.assertEqual(call.args[1]['endTime'], missing['close_ms'])
        self.assertLessEqual(call.args[1]['limit'], 2)
        bot.binance.get.reset_mock()
        await bot.fetch_candles(bot.states[state.symbol])
        bot.binance.get.assert_not_awaited()



class SenderSafetyTests(IsolatedAsyncioTestCase):
    async def run_one(self, bot):
        with patch('app.asyncio.sleep', side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):
                await bot.sender_loop()

    async def test_ab_delivery_preserves_body_and_durable_event_flags(self):
        bot, store, state = fixture()
        await bot.assess([state.symbol])
        row = next(iter(store.pending.values()))
        await self.run_one(bot)
        self.assertEqual(bot.telegram.send.await_args.args[0], row['body'])
        self.assertIsNone(bot.telegram.send.await_args.kwargs['reply_markup'])
        self.assertEqual(store.pending, {})
        self.assertTrue(bot.states[state.symbol].a_sent)

    async def test_new_notice_sends_buttons_and_syncs_durable_message_id(self):
        bot, store, state = fixture(status='PENDING')
        event = app.Event(f'notice:{state.symbol}:1', state.symbol, 'APPROVAL', NOW,
            'pending notice', reply_markup=bot.buttons(state.symbol, 1))
        await bot.save(events=[event])
        await self.run_one(bot)
        self.assertEqual(bot.telegram.send.await_args.kwargs['reply_markup'], event.reply_markup)
        self.assertEqual(bot.watch[state.symbol]['message_id'], 500)
        self.assertEqual(store.watch[state.symbol]['message_id'], 500)
        self.assertEqual(store.pending, {})

    async def test_resolved_notice_never_claims_still_pending(self):
        bot, store, state = fixture(status='PENDING')
        event = app.Event(f'notice:{state.symbol}:1', state.symbol, 'APPROVAL', NOW,
            'pending notice', reply_markup=bot.buttons(state.symbol, 1))
        await bot.save(events=[event])
        await bot.change_approval(state.symbol, 'BLOCKED')
        await self.run_one(bot)
        self.assertIn('BLOCKED', bot.telegram.send.await_args.args[0])
        self.assertEqual(bot.telegram.send.await_args.kwargs['reply_markup'], {'inline_keyboard': []})

    async def test_telegram_429_keeps_outbox_and_respects_retry_after(self):
        bot, store, state = fixture()
        await bot.assess([state.symbol])
        bot.telegram.send.side_effect = ApiFault(429, 120, 'Rate limited')
        await self.run_one(bot)
        self.assertEqual(len(store.pending), 1)
        row = next(iter(store.pending.values()))
        self.assertEqual(row['attempts'], 1)
        self.assertGreaterEqual(row['due_ms'], NOW + 120_000)
        self.assertTrue(bot.states[state.symbol].a_sent)

    async def test_repeated_service_diagnostics_do_not_flip_cause_detail(self):
        bot, store, state = fixture(status='BLOCKED')
        await bot.service_problem('WS:mark', 'CLOSED')
        await bot.diagnostics()
        first = copy.deepcopy(bot.incidents.snapshot())
        count = len(store.commits)
        await bot.service_problem('WS:mark', 'CLOSED')
        await bot.diagnostics()
        self.assertEqual(bot.incidents.snapshot(), first)
        self.assertEqual(len(store.commits), count)

    async def test_legacy_http_zero_does_not_replay_same_ws_failure(self):
        bot, store, state = fixture(status='BLOCKED')
        await bot.save([state])
        store.meta.update(diagnostics={'SERVICE:WS:mark': 'ApiFault HTTP 0'},
                          services={'WS:mark': 'ApiFault HTTP 0'})
        await bot.connect_db()
        await bot.service_problem('WS:mark', 'CLOSED')
        bot.now = lambda: NOW + 60_000
        await bot.diagnostics()
        self.assertFalse(any(e.kind == 'DIAGNOSTIC' for c in store.commits for e in c['events']))


    async def test_status_total_counts_current_trading_not_archived_contracts(self):
        bot, store, state = fixture()
        bot.meta['universe_stats'] = {'total': 700, 'trading': 525}
        text = await bot.status()
        self.assertIn('전체 USDT-M 무기한: 525', text)
        self.assertNotIn('전체 USDT-M 무기한: 700', text)


if __name__ == '__main__':
    unittest.main()
