"""Offline application integration for daily diagnostics and critical delivery."""
import copy
from datetime import datetime
from decimal import Decimal as D
import unittest
from unittest.mock import AsyncMock, Mock
import app
from api import ApiFault
from daily_report import DailyLedger, day_key, utf16_length
from engine import DAY_MS, Event
from test_engine import NOW, ready
from test_v4_runtime import MemoryStore, fixture


def at(value):
    return int(datetime.fromisoformat(value + '+09:00').timestamp()*1000)


class DailyMemoryStore(MemoryStore):
    """Application-facing fake; actual SQL transactions have separate tests."""
    def __init__(self):
        super().__init__()
        self.daily_snapshot = None
        self.closed_days, self.reports, self.legacy = {}, {}, {}
        self.daily_writes = 0
    async def load_daily(self):
        return copy.deepcopy(self.daily_snapshot)
    async def save_daily(self, snapshot, completed=()):
        if self.fail:
            raise OSError('offline daily storage failure')
        for day, payload in completed:
            self.closed_days.setdefault(day, copy.deepcopy(payload))
        self.daily_snapshot = copy.deepcopy(snapshot)
        self.daily_writes += 1
    async def daily_payload(self, day):
        return copy.deepcopy(self.closed_days.get(day))
    async def enqueue_daily(self, day, body, detected_ms, snapshot):
        if self.fail:
            raise OSError('offline daily registration failure')
        key = 'daily:' + day
        inserted = key not in self.reports
        if inserted:
            self.reports[key] = dict(id=key, body=body, symbol='', detected_ms=detected_ms,
                                     attempts=0, due_ms=0, sent_ms=None)
        self.daily_snapshot = copy.deepcopy(snapshot)
        return inserted
    async def first_daily(self, now):
        rows = [row for row in self.reports.values() if row['sent_ms'] is None and row['due_ms'] <= now]
        return copy.deepcopy(min(rows, key=lambda row: (row['detected_ms'], row['id']))) if rows else None
    async def retry_daily(self, event_id, attempts, due_ms):
        if self.fail:
            raise OSError('offline daily retry failure')
        if self.reports[event_id]['sent_ms'] is None:
            self.reports[event_id].update(attempts=attempts, due_ms=due_ms)
    async def complete_daily(self, event_id):
        if self.fail:
            raise OSError('offline daily acknowledgement failure')
        if self.reports[event_id]['sent_ms'] is None:
            self.reports[event_id]['sent_ms'] = 1
    @staticmethod
    def summary(rows):
        times = [row['detected_ms'] for row in rows]
        return dict(count=len(times), first_ms=min(times) if times else None,
                    last_ms=max(times) if times else None)
    async def migrate_daily_diagnostics(self, now):
        rows = [row for row in self.pending.values() if row['id'].startswith('diag:')]
        for row in rows:
            self.legacy.setdefault(row['id'], copy.deepcopy(row))
            del self.pending[row['id']]
        return self.summary(rows)
    async def legacy_summary(self):
        return self.summary(self.legacy.values())


def daily_fixture(clock=None, interval=1, status='BLOCKED'):
    clock = at('2026-10-24T12:00:00') if clock is None else clock
    bot, old, state = fixture(interval=interval, status=status)
    store = DailyMemoryStore()
    for name in ('states', 'histories', 'pending', 'meta', 'watch', 'commits', 'history_reads'):
        setattr(store, name, getattr(old, name))
    bot.store = store
    bot.clock_ms = clock
    bot.now = lambda: bot.clock_ms
    bot.started_ms = clock-120_000
    bot.daily = DailyLedger(clock)
    bot.daily_loaded, bot.daily_dirty = True, True
    bot.daily_closed = []
    bot.db_fault = None
    return bot, store, state


async def restart(bot, store, now):
    restored = app.Bot(store, Mock(), 'offline-fake-token', '100')
    restored.clock_ms = now
    restored.now = lambda: restored.clock_ms
    restored.started_ms = now-120_000
    restored.telegram.send = AsyncMock(return_value={'message_id': 501})
    await restored.connect_db()
    return restored


def queue_daily(store, now, key='daily:2026-10-23', body='daily report original'):
    store.reports[key] = dict(id=key, body=body, symbol='', detected_ms=now,
                             attempts=0, due_ms=0, sent_ms=None)
    return key


class DailyRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_persistence_crossing_midnight_closes_day_before_checkpoint(self):
        bot, store, _ = daily_fixture(at('2026-10-24T23:59:59'))
        await bot.service_problem('TELEGRAM', 'HTTP_502')
        bot.clock_ms = at('2026-10-25T00:00:00')
        async with bot.lock:
            await bot._persist_daily_locked(force=True)
        self.assertTrue(bot.ready)
        self.assertEqual(store.daily_snapshot['day'], '2026-10-25')
        old = store.closed_days['2026-10-24']['groups']['SERVICE:TELEGRAM:HTTP_502']
        self.assertEqual(old['events'], 1)
        self.assertEqual(old['last_ms'], at('2026-10-24T23:59:59'))

    async def test_full_critical_queue_does_not_invent_one_affected_symbol(self):
        bot, store, _ = daily_fixture()
        bot.pending_full = True
        await bot.diagnostics()
        group = bot.daily.groups['OUTBOX_FULL']
        self.assertEqual(group['events'], 1)
        self.assertIsNone(group['peak_impact'])
        self.assertTrue(group['impact_unknown'])
        self.assertFalse(store.pending)

    async def test_unchanged_fault_checkpoints_at_most_once_per_minute(self):
        bot, store, _ = daily_fixture()
        await bot.service_problem('WS:mark', 'CLOSED')
        initial = store.daily_writes
        for _ in range(3):
            bot.clock_ms += 15_000
            await bot.service_problem('WS:mark', 'CLOSED')
        self.assertEqual(store.daily_writes, initial)
        bot.clock_ms += 15_000
        await bot.service_problem('WS:mark', 'CLOSED')
        self.assertEqual(store.daily_writes, initial + 1)

    async def test_fault_visible_without_immediate_diagnostic(self):
        bot, store, _ = daily_fixture()
        await bot.service_problem('TELEGRAM', 'HTTP_429')
        await bot.diagnostics()
        self.assertEqual(bot.services['TELEGRAM'], 'HTTP_429')
        self.assertIn('TELEGRAM', await bot.status())
        self.assertEqual(store.daily_snapshot['groups']['SERVICE:TELEGRAM:HTTP_429']['events'], 1)
        self.assertFalse(store.pending)
        bot.telegram.send.assert_not_awaited()
        self.assertFalse(any(event.kind == 'DIAGNOSTIC' for write in store.commits for event in write['events']))

    async def test_repeat_is_one_event_and_recovered_recurrence_is_two(self):
        bot, store, _ = daily_fixture()
        for _ in range(8):
            bot.clock_ms += 15_000
            await bot.service_problem('TELEGRAM_POLL', 'TIMEOUT')
            await bot.diagnostics()
        key = 'SERVICE:TELEGRAM_POLL:TIMEOUT'
        self.assertEqual(bot.daily.groups[key]['events'], 1)
        await bot.service_problem('TELEGRAM_POLL')
        self.assertFalse(bot.daily.groups[key]['active'])
        bot.clock_ms += 1
        await bot.service_problem('TELEGRAM_POLL', 'TIMEOUT')
        self.assertEqual(bot.daily.groups[key]['events'], 2)
        self.assertEqual(bot.daily.groups[key]['recovered'], 1)
        self.assertFalse(store.pending)

    async def test_midnight_separates_235959_and_000000(self):
        bot, store, _ = daily_fixture(at('2026-10-24T23:59:59'))
        await bot.service_problem('TELEGRAM', 'HTTP_429')
        bot.clock_ms = at('2026-10-25T00:00:00')
        await bot.service_problem('TELEGRAM', 'HTTP_502')
        await bot.daily_reports_once()
        payload = store.closed_days['2026-10-24']
        self.assertEqual(payload['groups']['SERVICE:TELEGRAM:HTTP_429']['events'], 1)
        self.assertNotIn('SERVICE:TELEGRAM:HTTP_502', payload['groups'])
        self.assertEqual(bot.daily.groups['SERVICE:TELEGRAM:HTTP_502']['events'], 1)
        self.assertEqual(bot.daily.groups['SERVICE:TELEGRAM:HTTP_429']['events'], 0)
        report = store.reports['daily:2026-10-24']['body']
        self.assertIn('23:59:59', report)
        self.assertIn('2026-10-24 (KST)', report)
        self.assertNotIn('HTTP_502', report)
        self.assertEqual(day_key(bot.clock_ms), '2026-10-25')

    async def test_no_error_day_one_short_report_with_actual_health_counts(self):
        bot, store, _ = daily_fixture(at('2026-10-24T00:00:00'))
        await bot.diagnostics()
        bot.clock_ms = at('2026-10-25T00:00:00')
        await bot.daily_reports_once()
        await bot.daily_reports_once()
        self.assertEqual(list(store.reports), ['daily:2026-10-24'])
        body = store.reports['daily:2026-10-24']['body']
        self.assertIn('오늘 발생한 오류 없음', body)
        self.assertLess(utf16_length(body), 800)
        self.assertIn('감시 준비: 0 / 0', body)
        self.assertIn('가격 이력 준비: 0 / 0', body)
        self.assertIn('현재 미해결 오류: 0건', body)

    async def test_337_unknown_histories_one_incident_with_337_impact(self):
        bot, store, _ = daily_fixture(NOW, status='APPROVED')
        bot.states.clear()
        for symbol in bot.seed['approved']:
            state = ready(now=NOW)
            state.symbol, state.base = symbol, symbol[:-4]
            state.candles, state.history_ready, state.price = [], False, D('2')
            bot.states[symbol] = state
        await bot.diagnostics()
        bot.clock_ms += 15_000
        for state in bot.states.values():
            state.price_ms = state.funding_ms = bot.now()
        await bot.diagnostics()
        group = bot.daily.groups['PRICE_HISTORY_UNKNOWN']
        self.assertEqual(group['events'], 1)
        self.assertEqual(group['peak_impact'], 337)
        self.assertEqual(bot.daily.active_count(), 1)
        self.assertFalse(store.pending)
        self.assertIn('337', await bot.status())

    async def test_multiple_types_and_explicit_success_recovery(self):
        bot, store, _ = daily_fixture()
        for service, cause in [('WS:mark', 'CLOSED'), ('FUNDING_INFO', 'HTTP_502'),
                               ('TELEGRAM_POLL', 'TIMEOUT'), ('TELEGRAM', 'HTTP_429')]:
            await bot.service_problem(service, cause)
        self.assertEqual(sum(row['events'] for row in bot.daily.groups.values()), 4)
        await bot.service_problem('WS:mark')
        self.assertFalse(bot.daily.groups['SERVICE:WS:MARK:CLOSED']['active'])
        self.assertTrue(bot.daily.groups['SERVICE:TELEGRAM:HTTP_429']['active'])
        self.assertEqual(bot.daily.active_count(), 3)
        self.assertFalse(store.pending)

    async def test_restart_before_after_midnight_preserves_identity_and_data(self):
        before = at('2026-10-24T23:59:58')
        bot, store, state = daily_fixture(before)
        await bot.save([state], candles=True)
        await bot.service_problem('TELEGRAM_POLL', 'TIMEOUT')
        policy, history = copy.deepcopy(store.watch), copy.deepcopy(store.histories)
        restored = await restart(bot, store, before+1000)
        await restored.service_problem('TELEGRAM_POLL', 'TIMEOUT')
        self.assertEqual(restored.daily.groups['SERVICE:TELEGRAM_POLL:TIMEOUT']['events'], 1)
        restored.clock_ms = at('2026-10-25T00:00:00')
        await restored.daily_reports_once()
        again = await restart(restored, store, restored.clock_ms+1)
        await again.daily_reports_once()
        self.assertEqual(list(store.reports), ['daily:2026-10-24'])
        self.assertEqual(store.closed_days['2026-10-24']['groups']['SERVICE:TELEGRAM_POLL:TIMEOUT']['events'], 1)
        self.assertEqual(store.watch, policy)
        self.assertEqual(store.histories, history)
        self.assertEqual(sum(row['status'] == 'APPROVED' for row in policy.values()), 336)
        self.assertEqual(sum(row['status'] == 'BLOCKED' for row in policy.values()), 188)
        self.assertEqual(sum(row['status'] == 'REVIEW' for row in policy.values()), 1)

    async def test_offline_midnight_catchup_original_day_once_each(self):
        bot, store, state = daily_fixture(at('2026-10-24T23:59:59'))
        await bot.save([state], candles=True)
        await bot.service_problem('TELEGRAM', 'HTTP_429')
        restored = await restart(bot, store, at('2026-10-27T12:00:00'))
        await restored.daily_reports_once()
        self.assertEqual(len(store.reports), 1)
        for _ in range(3):
            await restored.daily_reports_once()
        self.assertEqual(set(store.reports), {'daily:2026-10-24', 'daily:2026-10-25', 'daily:2026-10-26'})
        self.assertIn('2026-10-24 (KST)', store.reports['daily:2026-10-24']['body'])
        self.assertIn('확인 불가', store.reports['daily:2026-10-25']['body'])
        self.assertNotIn('오늘 발생한 오류 없음', store.reports['daily:2026-10-25']['body'])

    async def test_legacy_diagnostic_archived_critical_rows_untouched(self):
        bot, store, state = daily_fixture()
        signal = Event('A:ALT:existing', state.symbol, 'A', bot.now(), 'original A body')
        notice = Event('notice:NEWUSDT:1', 'NEWUSDT', 'APPROVAL', bot.now(), 'new approval')
        diagnostic = Event('diag:old', '', 'DIAGNOSTIC', bot.now()-1000, 'PRICE_HISTORY_UNKNOWN: 337건')
        await bot.save([state], events=[signal, notice, diagnostic], candles=True)
        before = {key: copy.deepcopy(store.pending[key]) for key in (signal.id, notice.id)}
        restored = await restart(bot, store, bot.now()+1)
        self.assertEqual(store.pending, before)
        self.assertEqual(set(store.legacy), {'diag:old'})
        self.assertEqual(restored.daily.legacy_count, 1)
        self.assertNotIn('PRICE_HISTORY_UNKNOWN', restored.daily.groups)
        restored = await restart(restored, store, bot.now()+2)
        self.assertEqual(restored.daily.legacy_count, 1)
        self.assertEqual(store.pending, before)

    async def test_ab_signal_priority_original_body_and_no_duplicate(self):
        bot, store, state = daily_fixture(NOW, status='APPROVED')
        key = queue_daily(store, NOW-DAY_MS)
        await bot.assess([state.symbol])
        row = next(iter(store.pending.values()))
        self.assertTrue(await bot.sender_once())
        self.assertEqual(bot.telegram.send.await_args.args[0], row['body'])
        self.assertTrue(bot.states[state.symbol].a_sent)
        self.assertFalse(store.pending)
        self.assertIsNone(store.reports[key]['sent_ms'])
        await bot.assess([state.symbol])
        self.assertFalse(store.pending)
        await bot.sender_once()
        self.assertEqual(bot.telegram.send.await_args.args[0], 'daily report original')

    async def test_new_listing_notice_priority_with_buttons(self):
        bot, store, state = daily_fixture(NOW, status='PENDING')
        key = queue_daily(store, NOW-DAY_MS)
        markup = bot.buttons(state.symbol, 1)
        notice = Event('notice:'+state.symbol+':1', state.symbol, 'APPROVAL', NOW,
                       'new approval', reply_markup=markup)
        await bot.save(events=[notice])
        await bot.sender_once()
        self.assertEqual(bot.telegram.send.await_args.args[0], 'new approval')
        self.assertEqual(bot.telegram.send.await_args.kwargs['reply_markup'], markup)
        self.assertEqual(store.watch[state.symbol]['message_id'], 500)
        self.assertIsNone(store.reports[key]['sent_ms'])

    async def test_daily_queue_never_consumes_signal_capacity(self):
        bot, store, state = daily_fixture(NOW, status='APPROVED')
        store.max_pending = 1
        for index in range(50):
            queue_daily(store, 0, key=f'daily:old:{index}')
        self.assertEqual(await store.count_pending(), 0)
        await bot.assess([state.symbol])
        self.assertEqual(await store.count_pending(), 1)
        self.assertFalse(bot.pending_full)
        self.assertTrue(bot.states[state.symbol].a_sent)

    async def test_telegram_report_429_502_timeout_retry_one_report(self):
        for status, code, delay in [(429, 'HTTP_429', 120), (502, 'HTTP_502', 10), (0, 'TIMEOUT', 10)]:
            with self.subTest(code=code):
                bot, store, _ = daily_fixture()
                key = queue_daily(store, bot.now())
                bot.telegram.send.side_effect = ApiFault(status, delay, 'offline failure', reason_code=code)
                self.assertTrue(await bot.sender_once())
                row = store.reports[key]
                self.assertEqual(len(store.reports), 1)
                self.assertEqual(row['attempts'], 1)
                self.assertIsNone(row['sent_ms'])
                self.assertGreaterEqual(row['due_ms'], bot.now()+delay*1000)
                self.assertIn('SERVICE:TELEGRAM:'+code, bot.daily.groups)
                self.assertEqual(bot.daily.groups['SERVICE:TELEGRAM:'+code]['events'], 1)
                self.assertFalse(await bot.sender_once())
                bot.clock_ms = row['due_ms']
                bot.telegram.send.side_effect = None
                await bot.sender_once()
                self.assertEqual(bot.telegram.send.await_args.args[0], 'daily report original')
                self.assertIsNotNone(row['sent_ms'])
                self.assertFalse(bot.daily.groups['SERVICE:TELEGRAM:'+code]['active'])
                self.assertEqual(len(store.reports), 1)

    async def test_failed_daily_storage_keeps_dirty_record_and_db_fault_visible(self):
        bot, store, _ = daily_fixture()
        await bot.service_problem('TELEGRAM', 'HTTP_502')
        store.fail = True
        bot.clock_ms += 60_000
        with self.assertRaises(OSError):
            await bot.service_problem('TELEGRAM', 'HTTP_502')
        self.assertFalse(bot.ready)
        self.assertTrue(bot.daily_dirty)
        self.assertIn('DB', bot.services)
        self.assertEqual(bot.daily.groups['SERVICE:TELEGRAM:HTTP_502']['events'], 1)
        self.assertFalse(store.pending)


if __name__ == '__main__':
    unittest.main()
