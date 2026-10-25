"""Daily persistence tests use an in-memory SQL fake, never the operating DB."""
import copy
import json
import unittest

from engine import Event
from storage import Backpressure, DAILY_META_KEY, SCHEMA, Store
from test_policy_storage import FakeConnection


class DailyTransaction:
    names = ('watch', 'states', 'candles', 'meta', 'pending', 'days', 'reports', 'legacy')

    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        self.previous = {name: copy.deepcopy(getattr(self.connection, name)) for name in self.names}
        self.connection.depth += 1
        return self

    async def __aexit__(self, kind, value, trace):
        self.connection.depth -= 1
        if kind:
            for name, previous in self.previous.items():
                setattr(self.connection, name, previous)


class DailyConnection(FakeConnection):
    def __init__(self):
        super().__init__()
        self.days, self.reports, self.legacy = {}, {}, {}

    def transaction(self):
        return DailyTransaction(self)

    def check(self, sql, args):
        self.statements.append((sql, args))
        if self.fail and self.fail in sql:
            raise RuntimeError('injected SQL failure')

    @staticmethod
    def summary(rows):
        times = [row['detected_ms'] for row in rows]
        return dict(count=len(times), first_ms=min(times) if times else None,
                    last_ms=max(times) if times else None)

    async def fetchval(self, sql, *args):
        if sql.startswith('SELECT payload FROM bot_daily_days'):
            self.check(sql, args)
            return self.days.get(args[0])
        if sql.startswith('INSERT INTO bot_daily_reports'):
            self.check(sql, args)
            key, day, body, detected = args
            if any(row['day'] == day for row in self.reports.values()):
                return None
            if key in self.reports:
                raise RuntimeError('duplicate primary key')
            self.reports[key] = dict(id=key, day=day, body=body, detected_ms=detected,
                                     attempts=0, due_ms=0, sent_ms=None)
            return key
        return await super().fetchval(sql, *args)

    async def fetchrow(self, sql, *args):
        if sql.startswith('SELECT count(*) AS count'):
            self.check(sql, args)
            if 'FROM bot_pending' in sql:
                return self.summary(row for row in self.pending.values() if row['id'].startswith('diag:'))
            if 'FROM bot_daily_legacy' in sql:
                return self.summary(self.legacy.values())
        if sql.startswith('SELECT id,body,detected_ms,attempts,due_ms FROM bot_daily_reports'):
            self.check(sql, args)
            ready = [row for row in self.reports.values()
                     if row['sent_ms'] is None and row['due_ms'] <= args[0]]
            if not ready:
                return None
            row = min(ready, key=lambda item: (item['detected_ms'], item['id']))
            return {key: row[key] for key in ('id', 'body', 'detected_ms', 'attempts', 'due_ms')}
        return await super().fetchrow(sql, *args)

    async def execute(self, sql, *args):
        if sql.startswith('INSERT INTO bot_daily_days'):
            self.check(sql, args)
            self.days.setdefault(args[0], args[1])
        elif sql.startswith('UPDATE bot_daily_reports SET attempts='):
            self.check(sql, args)
            row = self.reports.get(args[0])
            if row and row['sent_ms'] is None:
                row.update(attempts=args[1], due_ms=args[2])
        elif sql.startswith('UPDATE bot_daily_reports SET sent_ms='):
            self.check(sql, args)
            row = self.reports.get(args[0])
            if row and row['sent_ms'] is None:
                row['sent_ms'] = args[1]
        elif sql.startswith('INSERT INTO bot_daily_legacy'):
            self.check(sql, args)
            for key, row in self.pending.items():
                if key.startswith('diag:'):
                    self.legacy.setdefault(key, dict(id=key, body=row['body'],
                        detected_ms=row['detected_ms'], migrated_ms=args[0]))
        elif sql.startswith('DELETE FROM bot_pending p'):
            self.check(sql, args)
            for key in tuple(self.pending):
                if key.startswith('diag:') and key in self.legacy:
                    del self.pending[key]
        else:
            return await super().execute(sql, *args)
        return 'OK'


class DailyStorageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.connection = DailyConnection()
        self.store = Store('offline-only', max_pending=1)
        self.store.conn = self.connection

    async def test_empty_daily_storage_and_additive_schema(self):
        self.assertIsNone(await self.store.load_daily())
        self.assertIsNone(await self.store.daily_payload('2026-10-24'))
        self.assertIsNone(await self.store.first_daily(1))
        self.assertEqual(await self.store.legacy_summary(),
                         dict(count=0, first_ms=None, last_ms=None))
        for table in ('bot_daily_days', 'bot_daily_reports', 'bot_daily_legacy'):
            self.assertIn('CREATE TABLE IF NOT EXISTS ' + table, SCHEMA)
        self.assertNotIn('DROP ', SCHEMA.upper())
        self.assertNotIn('TRUNCATE ', SCHEMA.upper())

    async def test_snapshot_and_closed_day_commit_together_and_are_restart_readable(self):
        snapshot = {'day': '2026-10-25', 'legacy_seen_total': 0}
        payload = {'day': '2026-10-24', 'events': {'PRICE_STALE': {'count': 1}}}
        await self.store.save_daily(snapshot, [('2026-10-24', payload)])
        self.assertEqual(await self.store.load_daily(), snapshot)
        self.assertEqual(await self.store.daily_payload('2026-10-24'), payload)
        other = Store('offline-only')
        other.conn = self.connection
        self.assertEqual(await other.load_daily(), snapshot)
        self.assertEqual(await other.daily_payload('2026-10-24'), payload)

    async def test_failed_snapshot_rolls_closed_day_back(self):
        self.connection.fail = 'INSERT INTO bot_meta'
        with self.assertRaises(RuntimeError):
            await self.store.save_daily({'day': '2026-10-25'}, [('2026-10-24', {'events': 1})])
        self.assertFalse(self.connection.days or self.connection.meta)

    async def test_closed_day_payload_is_immutable(self):
        await self.store.save_daily({'day': '2026-10-25'}, [('2026-10-24', {'events': 1})])
        await self.store.save_daily({'day': '2026-10-25'}, [('2026-10-24', {'events': 999})])
        self.assertEqual(await self.store.daily_payload('2026-10-24'), {'events': 1})

    async def test_report_fixed_id_duplicate_cursor_and_sent_marker_survive_restart(self):
        first = {'next_report_day': '2026-10-25'}
        self.assertTrue(await self.store.enqueue_daily('2026-10-24', 'original', 7, first))
        self.assertEqual(set(self.connection.reports), {'daily:2026-10-24'})
        self.assertFalse(await self.store.enqueue_daily('2026-10-24', 'must not overwrite', 9, first))
        self.assertEqual(self.connection.reports['daily:2026-10-24']['body'], 'original')
        self.assertEqual(await self.store.load_daily(), first)
        other = Store('offline-only')
        other.conn = self.connection
        await other.complete_daily('daily:2026-10-24')
        self.assertIsNone(await other.first_daily(10**15))
        self.assertFalse(await other.enqueue_daily('2026-10-24', 'duplicate', 12, first))
        self.assertEqual(len(self.connection.reports), 1)
        self.assertIsNotNone(self.connection.reports['daily:2026-10-24']['sent_ms'])

    async def test_registration_and_cursor_roll_back_on_insert_failure(self):
        self.connection.meta[DAILY_META_KEY] = json.dumps({'next_report_day': '2026-10-24'})
        self.connection.fail = 'INSERT INTO bot_daily_reports'
        with self.assertRaises(RuntimeError):
            await self.store.enqueue_daily('2026-10-24', 'report', 7, {'next_report_day': '2026-10-25'})
        self.assertFalse(self.connection.reports)
        self.assertEqual(await self.store.load_daily(), {'next_report_day': '2026-10-24'})

    async def test_registration_rolls_back_when_cursor_cannot_be_saved(self):
        self.connection.fail = 'INSERT INTO bot_meta'
        with self.assertRaises(RuntimeError):
            await self.store.enqueue_daily('2026-10-24', 'report', 7, {'next_report_day': '2026-10-25'})
        self.assertFalse(self.connection.reports or self.connection.meta)

    async def test_report_retries_preserve_body_id_and_due_order_without_duplicate_rows(self):
        await self.store.enqueue_daily('2026-10-24', 'original', 7, {})
        await self.store.enqueue_daily('2026-10-25', 'next day', 8, {})
        row = await self.store.first_daily(0)
        self.assertEqual(row['id'], 'daily:2026-10-24')
        self.assertEqual(row['symbol'], '')
        await self.store.retry_daily(row['id'], 3, 100)
        self.assertEqual((await self.store.first_daily(99))['id'], 'daily:2026-10-25')
        await self.store.complete_daily('daily:2026-10-25')
        row = await self.store.first_daily(100)
        self.assertEqual((row['id'], row['attempts'], row['due_ms'], row['body']),
                         ('daily:2026-10-24', 3, 100, 'original'))
        self.assertEqual(len(self.connection.reports), 2)

    async def test_ack_failure_leaves_report_available_for_safe_retry(self):
        await self.store.enqueue_daily('2026-10-24', 'report', 7, {})
        self.connection.fail = 'UPDATE bot_daily_reports SET sent_ms='
        with self.assertRaises(RuntimeError):
            await self.store.complete_daily('daily:2026-10-24')
        self.assertIsNone(self.connection.reports['daily:2026-10-24']['sent_ms'])
        self.assertEqual((await self.store.first_daily(0))['id'], 'daily:2026-10-24')

    async def test_late_retry_or_repeat_ack_never_reopens_a_sent_report(self):
        await self.store.enqueue_daily('2026-10-24', 'report', 7, {})
        await self.store.complete_daily('daily:2026-10-24')
        before = copy.deepcopy(self.connection.reports)
        await self.store.retry_daily('daily:2026-10-24', 10, 999)
        await self.store.complete_daily('daily:2026-10-24')
        self.assertEqual(self.connection.reports, before)
        self.assertIsNone(await self.store.first_daily(1000))

    async def test_daily_outbox_does_not_consume_signal_or_listing_capacity(self):
        await self.store.enqueue_daily('2026-10-24', 'report', 7, {})
        await self.store.commit(events=[Event('A:EX:1', 'EXUSDT', 'signal', 1, 'A')])
        self.assertEqual(await self.store.count_pending(), 1)
        self.assertEqual(set(self.connection.pending), {'A:EX:1'})
        with self.assertRaises(Backpressure):
            await self.store.commit(events=[Event('new:EX:1', 'EXUSDT', 'notice', 2, 'approval')])
        self.assertEqual(len(self.connection.reports), 1)

    def pending(self, key, body='diagnostic body with untrusted arbitrary counts', detected=1):
        self.connection.pending[key] = dict(id=key, body=body, symbol='EXUSDT', detected_ms=detected,
                                            attempts=2, due_ms=123, reply_markup=None)

    async def test_migration_archives_exact_diagnostic_prefix_and_preserves_critical_rows(self):
        self.pending('diag:old', detected=1)
        self.pending('diag:new', detected=10)
        self.pending('A:EX:1', body='diag: text is not a diagnostic id')
        self.pending('B:EX:1', body='DIAGNOSTIC')
        self.pending('new:EX:1', body='diag: approval')
        self.pending('diagnostic:unrelated', body='other')
        before = copy.deepcopy({key: row for key, row in self.connection.pending.items()
                                if not key.startswith('diag:')})
        self.connection.states = {'EXUSDT': 'state untouched'}
        self.connection.candles = {'EXUSDT': 'historic prices untouched'}
        self.connection.watch = {'EXUSDT': {'status': 'APPROVED', 'generation': 2}}
        summary = await self.store.migrate_daily_diagnostics(99)
        self.assertEqual(summary, dict(count=2, first_ms=1, last_ms=10))
        self.assertEqual(self.connection.pending, before)
        self.assertEqual(set(self.connection.legacy), {'diag:old', 'diag:new'})
        self.assertTrue(all(row['migrated_ms'] == 99 for row in self.connection.legacy.values()))
        self.assertEqual(self.connection.states, {'EXUSDT': 'state untouched'})
        self.assertEqual(self.connection.candles, {'EXUSDT': 'historic prices untouched'})
        self.assertEqual(self.connection.watch, {'EXUSDT': {'status': 'APPROVED', 'generation': 2}})
        self.assertEqual(await self.store.migrate_daily_diagnostics(101),
                         dict(count=0, first_ms=None, last_ms=None))
        self.assertEqual(await self.store.legacy_summary(), summary)

    async def test_migration_failed_archive_preserves_all_original_pending_rows(self):
        self.pending('diag:old')
        self.pending('A:EX:1')
        before = copy.deepcopy(self.connection.pending)
        self.connection.fail = 'INSERT INTO bot_daily_legacy'
        with self.assertRaises(RuntimeError):
            await self.store.migrate_daily_diagnostics(99)
        self.assertEqual(self.connection.pending, before)
        self.assertFalse(self.connection.legacy)

    async def test_migration_delete_failure_rolls_archive_back(self):
        self.pending('diag:old')
        before = copy.deepcopy(self.connection.pending)
        self.connection.fail = 'DELETE FROM bot_pending p'
        with self.assertRaises(RuntimeError):
            await self.store.migrate_daily_diagnostics(99)
        self.assertEqual(self.connection.pending, before)
        self.assertFalse(self.connection.legacy)

    async def test_cumulative_archive_summary_recovers_crash_before_ledger_save(self):
        self.pending('diag:old', detected=1)
        await self.store.migrate_daily_diagnostics(99)
        # Simulate crash: no collector state was saved after archiving.
        other = Store('offline-only')
        other.conn = self.connection
        self.assertIsNone(await other.load_daily())
        self.assertEqual(await other.legacy_summary(), dict(count=1, first_ms=1, last_ms=1))
        await other.save_daily({'legacy_seen_total': 1}, [('2026-10-24', {'legacy_messages': 1})])
        self.assertEqual((await other.load_daily())['legacy_seen_total'], 1)
        self.assertEqual((await other.daily_payload('2026-10-24'))['legacy_messages'], 1)

    async def test_dates_are_canonical_and_invalid_keys_do_not_write(self):
        for value in ('2026-1-24', '2026-02-30', '2026-10-24:extra', None):
            with self.assertRaises((TypeError, ValueError)):
                await self.store.enqueue_daily(value, 'invalid', 0, {})
        self.assertFalse(self.connection.reports or self.connection.meta)


if __name__ == '__main__':
    unittest.main()
