"""Offline tests for daily diagnostic aggregates and readable bounded reports."""
import json
import unittest

from daily_report import (DailyLedger, day_key, next_day, start_ms,
                          render_report, utf16_length)


DAY = '2026-10-25'
T0 = start_ms(DAY)


class DailyLedgerTests(unittest.TestCase):
    def test_integer_helpers_kst_boundary(self):
        self.assertEqual(day_key(T0 - 1), '2026-10-24')
        self.assertEqual(day_key(T0), DAY)
        self.assertEqual(day_key(T0 + 86_400_000 - 1), DAY)
        self.assertEqual(next_day('2026-12-31'), '2027-01-01')
        self.assertEqual(next_day('2028-02-28'), '2028-02-29')

    def test_no_errors(self):
        ledger = DailyLedger(T0)
        body = render_report(DAY, ledger.day_payload(), T0 + 86_400_000)
        self.assertIn('오늘 발생한 오류 없음', body)
        self.assertLessEqual(utf16_length(body), 3800)

    def test_repeated_observations_one_incident(self):
        ledger = DailyLedger(T0)
        ledger.observe('PRICE_STALE', 4, T0 + 1000)
        for seconds in (15, 30, 45, 60):
            ledger.observe('PRICE_STALE', 4, T0 + seconds * 1000)
        group = ledger.groups['PRICE_STALE']
        self.assertEqual(group['events'], 1)
        self.assertEqual(group['first_ms'], T0 + 1000)
        self.assertEqual(group['last_ms'], T0 + 60_000)
        self.assertEqual(ledger.active_count(), 1)

    def test_healthy_to_healthy_does_not_write(self):
        ledger = DailyLedger(T0)
        self.assertFalse(ledger.resolve('SERVICE:WS:MARK:CLOSED', T0))
        self.assertFalse(ledger.resolve('SERVICE:WS:MARK:CLOSED', T0 + 1000))

    def test_recovery_then_recurrence_is_separate_even_within_30_minutes(self):
        ledger = DailyLedger(T0)
        ledger.observe('PRICE_STALE', 1, T0)
        self.assertTrue(ledger.resolve('PRICE_STALE', T0 + 1000))
        self.assertFalse(ledger.resolve('PRICE_STALE', T0 + 2000))
        ledger.observe('PRICE_STALE', 2, T0 + 3000)
        self.assertEqual(ledger.groups['PRICE_STALE']['events'], 2)
        self.assertEqual(ledger.groups['PRICE_STALE']['recovered'], 1)
        self.assertEqual(ledger.groups['PRICE_STALE']['peak_impact'], 2)

    def test_337_symbols_are_one_incident_with_scope(self):
        ledger = DailyLedger(T0)
        ledger.observe('PRICE_HISTORY_UNKNOWN', 337, T0)
        ledger.observe('PRICE_HISTORY_UNKNOWN', 300, T0 + 15_000)
        group = ledger.groups['PRICE_HISTORY_UNKNOWN']
        self.assertEqual(group['events'], 1)
        self.assertEqual(group['peak_impact'], 337)
        body = render_report(DAY, ledger.day_payload(), T0 + 86_400_000)
        self.assertIn('발생 사건: 1회', body)
        self.assertIn('최대 동시 영향: 337개', body)

    def test_simultaneous_fault_causes_do_not_merge(self):
        ledger = DailyLedger(T0)
        for kind in ('PRICE_STALE', 'PRICE_HISTORY_UNKNOWN', 'SERVICE:TELEGRAM:HTTP_429',
                     'SERVICE:TELEGRAM:HTTP_502', 'SERVICE:TELEGRAM:TIMEOUT'):
            ledger.observe(kind, None, T0)
        self.assertEqual(sum(g['events'] for g in ledger.groups.values()), 5)
        self.assertEqual(ledger.active_count(), 5)

    def test_unknown_scope_never_becomes_zero(self):
        ledger = DailyLedger(T0)
        ledger.observe('SERVICE:DB:NETWORK', None, T0)
        self.assertIsNone(ledger.groups['SERVICE:DB:NETWORK']['peak_impact'])
        self.assertIn('최대 동시 영향: 확인 불가개',
                      render_report(DAY, ledger.day_payload(), T0))

    def test_known_peak_and_unknown_observation_both_retained(self):
        ledger = DailyLedger(T0)
        ledger.observe('PRICE_STALE', 3, T0)
        ledger.observe('PRICE_STALE', None, T0 + 1000)
        group = ledger.groups['PRICE_STALE']
        self.assertEqual(group['peak_impact'], 3)
        self.assertTrue(group['impact_unknown'])

    def test_235959_is_old_day_000000_is_new_day(self):
        ledger = DailyLedger(T0)
        ledger.observe('PRICE_STALE', 4, T0 + 86_399_000)
        closed = ledger.roll(T0 + 86_400_000)
        ledger.observe('PRICE_HISTORY_UNKNOWN', 337, T0 + 86_400_000)
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0][0], DAY)
        self.assertEqual(closed[0][1]['groups']['PRICE_STALE']['events'], 1)
        self.assertNotIn('PRICE_HISTORY_UNKNOWN', closed[0][1]['groups'])
        self.assertEqual(ledger.day, next_day(DAY))
        self.assertEqual(ledger.groups['PRICE_HISTORY_UNKNOWN']['events'], 1)

    def test_ongoing_fault_crosses_midnight_without_new_onset(self):
        ledger = DailyLedger(T0)
        ledger.observe('PRICE_STALE', 4, T0 + 86_399_000)
        ledger.roll(T0 + 86_400_000)
        ledger.observe('PRICE_STALE', 2, T0 + 86_415_000)
        group = ledger.groups['PRICE_STALE']
        self.assertEqual(group['events'], 0)
        self.assertEqual(group['continued'], 1)
        self.assertEqual(group['first_ms'], T0 + 86_415_000)

    def test_closed_payload_is_independent_of_new_day_mutations(self):
        ledger = DailyLedger(T0)
        ledger.observe('PRICE_STALE', 4, T0)
        closed = ledger.roll(T0 + 86_400_000)
        ledger.resolve('PRICE_STALE', T0 + 86_401_000)
        self.assertTrue(closed[0][1]['groups']['PRICE_STALE']['active'])

    def test_report_cursor_not_advanced_before_enqueue_transaction(self):
        ledger = DailyLedger(T0)
        ledger.roll(T0 + 86_400_000)
        self.assertEqual(ledger.next_report_day, DAY)
        ledger.next_report_day = next_day(DAY)
        self.assertEqual(ledger.snapshot()['next_report_day'], next_day(DAY))

    def test_same_day_roll_is_idempotent(self):
        ledger = DailyLedger(T0)
        self.assertEqual(ledger.roll(T0 + 1000), [])
        self.assertEqual(ledger.roll(T0 + 86_400_000)[0][0], DAY)
        self.assertEqual(ledger.roll(T0 + 86_401_000), [])

    def test_multi_day_gap_returns_only_tracked_day(self):
        ledger = DailyLedger(T0)
        closed = ledger.roll(T0 + 4 * 86_400_000)
        self.assertEqual([day for day, _ in closed], [DAY])
        self.assertTrue(ledger.coverage_unknown)
        self.assertEqual(ledger.day, '2026-10-29')

    def test_restart_before_midnight_preserves_incidents_and_marks_gap(self):
        ledger = DailyLedger(T0)
        ledger.observe('PRICE_STALE', 4, T0 + 86_398_000)
        saved = json.loads(json.dumps(ledger.snapshot()))
        restored = DailyLedger(T0 + 86_399_000, saved, restarted=True)
        self.assertEqual(restored.groups['PRICE_STALE']['events'], 1)
        self.assertTrue(restored.groups['PRICE_STALE']['unknown'])
        self.assertTrue(restored.coverage_unknown)
        restored.observe('PRICE_STALE', 4, T0 + 86_399_500)
        self.assertEqual(restored.groups['PRICE_STALE']['events'], 1)
        self.assertFalse(restored.groups['PRICE_STALE']['unknown'])

    def test_restart_after_midnight_has_no_invented_recovery(self):
        ledger = DailyLedger(T0)
        ledger.observe('PRICE_STALE', 1, T0 + 86_399_000)
        restored = DailyLedger(T0 + 86_401_000, ledger.snapshot(), restarted=True)
        closed = restored.roll(T0 + 86_401_000)
        self.assertTrue(closed[0][1]['groups']['PRICE_STALE']['unknown'])
        self.assertTrue(restored.groups['PRICE_STALE']['unknown'])
        self.assertEqual(restored.groups['PRICE_STALE']['events'], 0)
        restored.resolve('PRICE_STALE', T0 + 86_402_000)
        self.assertEqual(restored.active_count(), 0)

    def test_missing_day_is_unknown_not_clean(self):
        body = render_report(DAY, None, T0 + 3 * 86_400_000)
        self.assertIn('확인 불가', body)
        self.assertIn('지연 보고', body)
        self.assertNotIn('오늘 발생한 오류 없음', body)

    def test_partial_coverage_zero_is_not_false_clean_day(self):
        ledger = DailyLedger(T0)
        ledger.mark_gap(T0)
        body = render_report(DAY, ledger.day_payload(), T0 + 86_400_000)
        self.assertIn('전체 기간 오류 여부는 확인 불가', body)
        self.assertNotIn('오늘 발생한 오류 없음', body)

    def test_migrated_messages_are_not_invented_incidents(self):
        ledger = DailyLedger(T0)
        ledger.add_legacy(12, T0 - 86_400_000, T0 - 1000, T0)
        ledger.legacy_seen_total = 12
        payload = ledger.day_payload()
        self.assertEqual(payload['legacy_count'], 12)
        self.assertEqual(payload['groups'], {})
        self.assertEqual(ledger.snapshot()['legacy_seen_total'], 12)
        text = render_report(DAY, payload, T0)
        self.assertIn('실제 장애 사건 수는 확인 불가', text)
        self.assertIn('진단 12개 전환', text)

    def test_many_real_service_causes_compact_losslessly(self):
        ledger = DailyLedger(T0)
        services = ('CLOCK', 'EXCHANGE', 'FUNDING_INFO', 'FUNDING_HISTORY', 'LIVE_RECHECK',
                    'KLINES', 'WS:MARK', 'WS:PRICE', 'TELEGRAM', 'TELEGRAM_POLL', 'DB')
        faults = ('HTTP_429', 'HTTP_502', 'TIMEOUT', 'NETWORK', 'CLOSED', 'HTTP_403', 'DATA')
        kinds = [f'SERVICE:{service}:{fault}' for service in services for fault in faults]
        for kind in kinds:
            ledger.observe(kind, 337, T0)
        body = render_report(DAY, ledger.day_payload(), T0 + 86_400_000)
        self.assertLessEqual(utf16_length(body), 3800)
        self.assertIn('신규 사건: 77회', body)
        self.assertIn('영향최대337', body)
        for kind in kinds:
            self.assertIn(kind, body)

    def test_health_values_are_actual_or_explicitly_unknown(self):
        ledger = DailyLedger(T0)
        body = render_report(DAY, ledger.day_payload(), T0,
                             {'ready': 330, 'history': 335, 'approved': 337,
                              'pending': 2, 'unresolved': 4})
        self.assertIn('감시 준비: 330 / 337', body)
        self.assertIn('가격 이력 준비: 335 / 337', body)
        self.assertIn('미전송: 2건', body)
        self.assertIn('현재 미해결 오류: 4건', body)
        body_unknown = render_report(DAY, ledger.day_payload(), T0)
        self.assertIn('감시 준비: 확인 불가 / 확인 불가', body_unknown)

    def test_utf16_counts_emoji_correctly(self):
        self.assertEqual(utf16_length('📋'), 2)
        self.assertEqual(utf16_length('오류'), 2)

    def test_snapshot_does_not_alias_live_mutable_state(self):
        ledger = DailyLedger(T0)
        ledger.observe('PRICE_STALE', 4, T0)
        clone = ledger.snapshot()
        clone['groups']['PRICE_STALE']['events'] = 500
        self.assertEqual(ledger.groups['PRICE_STALE']['events'], 1)

    def test_raw_details_cannot_enter_cause_names(self):
        ledger = DailyLedger(T0)
        ledger.observe('https://secret.example/bot-token?value=secret', None, T0)
        self.assertIn('OTHER:UNCLASSIFIED', ledger.groups)
        self.assertNotIn('secret', json.dumps(ledger.snapshot()))

    def test_repeated_failure_updates_last_in_memory_without_dirty_write(self):
        ledger = DailyLedger(T0)
        self.assertTrue(ledger.observe('PRICE_STALE', 4, T0))
        self.assertFalse(ledger.observe('PRICE_STALE', 4, T0 + 15_000))
        self.assertEqual(ledger.groups['PRICE_STALE']['last_ms'], T0 + 15_000)
        self.assertEqual(ledger.active['PRICE_STALE']['last_ms'], T0 + 15_000)

    def test_first_activation_midday_is_partial_coverage(self):
        ledger = DailyLedger(T0 + 12 * 3_600_000)
        self.assertTrue(ledger.coverage_unknown)
        self.assertNotIn('오늘 발생한 오류 없음',
                         render_report(DAY, ledger.day_payload(), T0 + 86_400_000))

    def test_many_real_causes_with_unique_metrics_still_readable(self):
        ledger = DailyLedger(T0)
        services = ('CLOCK', 'EXCHANGE', 'FUNDING_INFO', 'FUNDING_HISTORY', 'LIVE_RECHECK',
                    'KLINES', 'WS:MARK', 'WS:PRICE', 'TELEGRAM', 'TELEGRAM_POLL', 'DB')
        faults = ('HTTP_429', 'HTTP_502', 'TIMEOUT', 'NETWORK', 'CLOSED', 'HTTP_403', 'DATA')
        kinds = [f'SERVICE:{service}:{fault}' for service in services for fault in faults]
        for index, kind in enumerate(kinds):
            ledger.observe(kind, 337, T0 + index * 1000)
        body = render_report(DAY, ledger.day_payload(), T0 + 86_400_000)
        self.assertLessEqual(utf16_length(body), 3800)
        self.assertNotIn('zlib+base64', body)
        self.assertEqual(body.count('|1/0|337|'), 77)
        self.assertIn('신규 사건: 77회', body)
        self.assertIn('000000|A', body)
        self.assertIn('000116|A', body)

    def test_integer_day_helpers_support_millisecond_edges(self):
        for day in ('1970-01-01', '2000-02-29', '2026-10-25', '2099-12-31'):
            beginning = start_ms(day)
            self.assertEqual(day_key(beginning), day)
            self.assertEqual(day_key(beginning + 86_399_999), day)
            self.assertEqual(day_key(beginning + 86_400_000), next_day(day))

    def test_current_health_not_inferred_from_historical_report_payload(self):
        ledger = DailyLedger(T0)
        ledger.observe('PRICE_STALE', 4, T0)
        body = render_report(DAY, ledger.day_payload(), T0 + 2 * 86_400_000)
        self.assertIn('현재 미해결 오류: 확인 불가건', body)

    def test_restart_at_0200_next_day_marks_current_day_gap(self):
        ledger = DailyLedger(T0)
        ledger.checkpoint(T0 + 86_399_000)
        restored = DailyLedger(T0 + 86_400_000 + 2 * 3_600_000,
                               ledger.snapshot(), restarted=True)
        closed = restored.roll(T0 + 86_400_000 + 2 * 3_600_000)
        self.assertTrue(closed[0][1]['coverage_unknown'])
        self.assertTrue(restored.coverage_unknown)
        self.assertNotIn('오늘 발생한 오류 없음',
                         render_report(restored.day, restored.day_payload(), T0 + 2 * 86_400_000))

    def test_old_gap_does_not_poison_future_fully_observed_days(self):
        ledger = DailyLedger(T0)
        restored = DailyLedger(T0 + 86_400_000 + 2 * 3_600_000,
                               ledger.snapshot(), restarted=True)
        restored.roll(T0 + 86_400_000 + 2 * 3_600_000)
        restored.checkpoint(T0 + 2 * 86_400_000 - 30_000)
        restored.roll(T0 + 2 * 86_400_000)
        self.assertFalse(restored.coverage_unknown)

    def test_roll_during_long_database_checkpoint_gap_is_unknown(self):
        ledger = DailyLedger(T0)
        ledger.checkpoint(T0 + 86_400_000 - 180_000)
        ledger.roll(T0 + 86_400_000)
        self.assertTrue(ledger.coverage_unknown)


if __name__ == '__main__':
    unittest.main()
