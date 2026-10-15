"""Domain scenarios use mock values only and never call any live API."""
import json
import unittest
from dataclasses import replace
from decimal import Decimal as D

from engine import (Candle, DAY_MS, FOUR_HOURS_MS, HOUR_MS,
                    State, evaluate, from_dict, minimum_close, problems, to_dict)

NOW = 1_800_000_000_000


def fill_history(state, now=NOW, *, low=D('1')):
    lower = max(state.onboard_ms, now - 90 * DAY_MS)
    first_open = lower // FOUR_HOURS_MS * FOUR_HOURS_MS
    last_open = now // FOUR_HOURS_MS * FOUR_HOURS_MS - FOUR_HOURS_MS
    state.candles = [Candle(opened, opened + FOUR_HOURS_MS - 1, low)
                     for opened in range(first_open, last_open + 1, FOUR_HOURS_MS)
                     if opened + FOUR_HOURS_MS - 1 >= lower]
    state.history_ready = True
    state.history_revision += 1


def ready(interval=1, now=NOW, onboard=None):
    state = State('ALTUSDT', 'ALT', onboard or now - 100 * DAY_MS,
                  name='독립 프로젝트', price=D('3'), price_ms=now,
                  funding=D('-0.02'), funding_ms=now,
                  next_funding_ms=now + HOUR_MS, interval_hours=interval,
                  interval_ms=now, interval_source='fundingInfo', floor=D('-0.02'),
                  floor_ms=now, floor_source='fundingInfo')
    fill_history(state, now)
    return state


def refresh(state, now, funding=None):
    state.funding_ms = now
    state.price_ms = now
    state.interval_ms = now
    state.floor_ms = now
    if funding is not None:
        state.funding = funding


class DomainTests(unittest.TestCase):
    def assert_kind(self, state, kind, now=NOW):
        events = evaluate(state, now)
        self.assertEqual([event.kind for event in events], [kind])
        return events[0]

    def test_01_price_2_999_rejected(self):
        state = ready()
        state.price = D('2.999')
        self.assertEqual(evaluate(state, NOW), [])

    def test_02_price_exact_3_accepted(self):
        self.assert_kind(ready(), 'A')

    def test_03_one_hour_price_arrives_later(self):
        state = ready()
        state.price = D('2.99')
        self.assertEqual(evaluate(state, NOW), [])
        epoch = state.a_epoch
        state.price = D('3')
        self.assert_kind(state, 'A')
        self.assertEqual(state.a_epoch, epoch)

    def test_04_four_hour_floor_reached(self):
        self.assert_kind(ready(4), 'B')

    def test_05_eight_hour_floor_reached(self):
        self.assert_kind(ready(8), 'B')

    def test_06_b_then_one_hour_a(self):
        state = ready(4)
        first = self.assert_kind(state, 'B')
        state.interval_hours = 1
        second = self.assert_kind(state, 'A')
        self.assertNotEqual(first.id, second.id)
        self.assertIsNone(state.b_epoch)
        self.assertEqual(state.previous_interval, 4)

    def test_07_b_needs_no_one_hour_transition(self):
        state = ready(8)
        self.assert_kind(state, 'B')
        for step in (1_000, 30_000, 60_000):
            refresh(state, NOW + step)
            self.assertEqual(evaluate(state, NOW + step), [])
        self.assertEqual(state.interval_hours, 8)

    def test_08_a_persists_when_price_and_funding_change(self):
        state = ready()
        first = self.assert_kind(state, 'A')
        state.price = D('2')
        state.funding = D('0.01')
        self.assertEqual(evaluate(state, NOW), [])
        state.price = D('3')
        state.funding = D('-0.02')
        self.assertEqual(evaluate(state, NOW), [])
        self.assertTrue(first.id.endswith(state.a_epoch))

    def test_09_one_to_four_to_one_creates_new_a(self):
        state = ready()
        first = self.assert_kind(state, 'A')
        state.interval_hours = 4
        evaluate(state, NOW)
        state.interval_hours = 1
        second = self.assert_kind(state, 'A')
        self.assertNotEqual(first.id, second.id)

    def test_10_recovery_less_than_60_seconds_keeps_event(self):
        state = ready(4)
        first = self.assert_kind(state, 'B')
        for step in (1_000, 11_000, 21_000, 31_000, 41_000, 51_000, 60_000):
            refresh(state, NOW + step, D('-0.018'))
            self.assertEqual(evaluate(state, NOW + step), [])
        refresh(state, NOW + 61_000, D('-0.02'))
        self.assertEqual(evaluate(state, NOW + 61_000), [])
        self.assertTrue(first.id.endswith(state.b_epoch))

    def test_11_recovery_exactly_60_seconds_rearms_b(self):
        state = ready(4)
        first = self.assert_kind(state, 'B')
        for step in range(1_000, 61_001, 10_000):
            refresh(state, NOW + step, D('-0.018'))
            self.assertEqual(evaluate(state, NOW + step), [])
        self.assertIsNone(state.b_epoch)
        refresh(state, NOW + 62_000, D('-0.02'))
        second = self.assert_kind(state, 'B', NOW + 62_000)
        self.assertNotEqual(first.id, second.id)

    def test_12_floor_numeric_change_new_b(self):
        state = ready(4)
        first = self.assert_kind(state, 'B')
        state.floor = D('-0.01')
        second = self.assert_kind(state, 'B')
        self.assertNotEqual(first.id, second.id)

    def test_floor_change_alone_does_not_emit(self):
        state = ready(4)
        self.assert_kind(state, 'B')
        state.floor = D('-0.03')
        self.assertEqual(evaluate(state, NOW), [])
        state.funding = D('-0.03')
        self.assert_kind(state, 'B')

    def test_same_floor_new_source_does_not_duplicate(self):
        state = ready(4)
        self.assert_kind(state, 'B')
        state.floor_source = 'official rule'
        state.floor_estimated = True
        self.assertEqual(evaluate(state, NOW), [])

    def test_13_b_price_arrives_after_floor(self):
        state = ready(8)
        state.price = D('2.999')
        self.assertEqual(evaluate(state, NOW), [])
        epoch = state.b_epoch
        state.price = D('3')
        self.assert_kind(state, 'B')
        self.assertEqual(state.b_epoch, epoch)

    def test_14_before_168_hours_blocked(self):
        state = ready(onboard=NOW - 168 * HOUR_MS + 1)
        self.assertEqual(evaluate(state, NOW), [])

    def test_15_exact_168_hours_allowed(self):
        self.assert_kind(ready(onboard=NOW - 168 * HOUR_MS), 'A')

    def test_16_listing_first_seven_days_included(self):
        state = ready(onboard=NOW - 8 * DAY_MS)
        fill_history(state, low=D('2'))
        state.candles[0] = replace(state.candles[0], close=D('1'))
        self.assertEqual(minimum_close(state, NOW), D('1'))
        self.assert_kind(state, 'A')

    def test_17_older_than_90_days_excluded(self):
        state = ready()
        old = state.candles[0]
        state.candles.insert(0, Candle(old.open_ms - FOUR_HOURS_MS,
                                      old.close_ms - FOUR_HOURS_MS, D('0.01')))
        self.assertEqual(minimum_close(state, NOW), D('1'))

    def test_moving_low_expires(self):
        state = ready()
        fill_history(state, low=D('2'))
        state.candles[0] = replace(state.candles[0], close=D('1'))
        self.assertEqual(minimum_close(state, NOW), D('1'))
        next_now = NOW + FOUR_HOURS_MS
        fill_history(state, next_now, low=D('2'))
        self.assertEqual(minimum_close(state, next_now), D('2'))

    def test_18_legacy_fdv_metadata_cannot_change_signal_rules(self):
        data = to_dict(ready())
        data.update(market_cap='9500', fdv='10000', cg_checked_ms=NOW)
        self.assert_kind(from_dict(data), 'A')

    def test_19_ratio_09499_allowed(self):
        self.assert_kind(ready(), 'A')

    def test_20_legacy_missing_fdv_does_not_gate_engine(self):
        for value in (None, '0', '-1', 'NaN', 'Infinity'):
            data = to_dict(ready())
            data['fdv'] = value
            self.assert_kind(from_dict(data), 'A')

    def test_21_legacy_classification_does_not_gate_engine(self):
        data = to_dict(ready())
        data['classification'] = 'STOCK'
        self.assert_kind(from_dict(data), 'A')

    def test_22_approval_policy_not_named_base_gates_engine(self):
        for base in ('BTC', 'ETH', 'XRP', 'SOL', 'TRX', 'DOGE', 'HYPE', 'BNB'):
            state = ready()
            state.base = base
            self.assert_kind(state, 'A')

    def test_23_ecosystem_utility_not_excluded(self):
        for base in ('JUP', 'ARB', 'OP', 'UNI', 'BONK'):
            state = ready()
            state.base = base
            self.assert_kind(state, 'A')

    def test_24_legacy_mapping_fields_are_ignored_on_load(self):
        data = to_dict(ready())
        data.update(mapping_valid=False, cg_id=None)
        self.assert_kind(from_dict(data), 'A')

    def test_25_unknown_interval_no_false_signal(self):
        for value in (None, 2, 0, True, '1'):
            state = ready()
            state.interval_hours = value
            self.assertEqual(evaluate(state, NOW), [])
            self.assertIn('INTERVAL_UNKNOWN', problems(state, NOW))

    def test_unknown_interval_does_not_reset_known_a_segment(self):
        state = ready()
        first = self.assert_kind(state, 'A')
        state.interval_hours = None
        self.assertEqual(evaluate(state, NOW), [])
        state.interval_hours = 1
        self.assertEqual(evaluate(state, NOW), [])
        self.assertTrue(first.id.endswith(state.a_epoch))

    def test_stale_interval_does_not_reset_known_a_segment(self):
        state = ready()
        self.assert_kind(state, 'A')
        state.interval_ms = NOW - 300_001
        self.assertEqual(evaluate(state, NOW), [])
        state.interval_ms = NOW
        self.assertEqual(evaluate(state, NOW), [])

    def test_27_stale_streams_block_at_exact_threshold(self):
        for field_name, age, key in (('funding_ms', 15_000, 'FUNDING_STALE'),
                                     ('price_ms', 120_000, 'PRICE_STALE')):
            state = ready()
            setattr(state, field_name, NOW - age)
            self.assertEqual(evaluate(state, NOW), [])
            self.assertIn(key, problems(state, NOW))

    def test_fresh_streams_one_ms_before_threshold(self):
        state = ready()
        state.funding_ms = NOW - 14_999
        state.price_ms = NOW - 119_999
        self.assert_kind(state, 'A')

    def test_30_restart_dedup_a_and_b(self):
        for interval in (1, 4, 8):
            state = ready(interval)
            first = evaluate(state, NOW)[0]
            serialized = json.loads(json.dumps(to_dict(state)))
            restored = from_dict(serialized)
            self.assertEqual(evaluate(restored, NOW), [])
            self.assertEqual(to_dict(restored), to_dict(state))
            self.assertTrue(first.id.endswith(restored.a_epoch or restored.b_epoch))

    def test_31_history_and_auxiliary_storage_bounded(self):
        state = ready()
        self.assertLessEqual(len(state.candles), 541)
        state.next_times = list(range(100))
        data = to_dict(state)
        self.assertEqual(data['next_times'], list(range(92, 100)))
        self.assertEqual(len(from_dict(data).next_times), 8)

    def test_32_normal_state_no_diagnostic_or_periodic_message(self):
        state = ready()
        state.funding = D('0.001')
        self.assertEqual(evaluate(state, NOW), [])
        self.assertEqual(problems(state, NOW), {})

    def test_a_independent_of_unknown_floor(self):
        state = ready()
        state.floor = None
        state.floor_ms = 0
        self.assert_kind(state, 'A')
        self.assertNotIn('FLOOR_UNKNOWN', problems(state, NOW))

    def test_b_unknown_floor_diagnostic(self):
        state = ready(4)
        state.floor = None
        self.assertEqual(evaluate(state, NOW), [])
        self.assertIn('FLOOR_UNKNOWN', problems(state, NOW))

    def test_estimated_b_message_and_original_detection_time(self):
        state = ready(8)
        state.floor_estimated = True
        state.floor_source = '최근 30일 관측 최저 음수 정산값'
        event = self.assert_kind(state, 'B')
        self.assertIn('추정 하한: -2%', event.message)
        self.assertIn('KST', event.message)
        self.assertIn('감지 시각:', event.message)
        self.assertIn('다음 정산 예정시각:', event.message)
        self.assertIn('https://www.binance.com/en/futures/ALTUSDT', event.message)
        self.assertEqual(event.detected_ms, NOW)

    def test_funding_exact_zero_is_not_negative(self):
        for interval in (1, 4, 8):
            state = ready(interval)
            state.funding = D('0')
            self.assertEqual(evaluate(state, NOW), [])

    def test_positive_funding_then_negative_emits_a(self):
        state = ready()
        state.funding = D('0.001')
        self.assertEqual(evaluate(state, NOW), [])
        state.funding = D('-0.00001')
        self.assert_kind(state, 'A')

    def test_floor_numeric_precision_no_epsilon(self):
        state = ready(4)
        state.funding = D('-0.01999999999999999999999999999')
        self.assertEqual(evaluate(state, NOW), [])
        state.funding = D('-0.02000000000000000000000000001')
        self.assert_kind(state, 'B')

    def test_recovery_less_than_10_percent_does_not_rearm(self):
        state = ready(4)
        self.assert_kind(state, 'B')
        for step in range(1_000, 101_001, 10_000):
            refresh(state, NOW + step, D('-0.01800000001'))
            self.assertEqual(evaluate(state, NOW + step), [])
        refresh(state, NOW + 102_000, D('-0.02'))
        self.assertEqual(evaluate(state, NOW + 102_000), [])

    def test_recovery_gap_does_not_invent_continuity(self):
        state = ready(4)
        self.assert_kind(state, 'B')
        refresh(state, NOW + 1_000, D('-0.018'))
        evaluate(state, NOW + 1_000)
        refresh(state, NOW + 61_000, D('-0.018'))
        evaluate(state, NOW + 61_000)
        self.assertEqual(state.recovery_started_ms, NOW + 61_000)
        self.assertIsNotNone(state.b_epoch)
        refresh(state, NOW + 62_000, D('-0.02'))
        self.assertEqual(evaluate(state, NOW + 62_000), [])

    def test_recovery_gap_at_15_seconds_resets(self):
        state = ready(4)
        self.assert_kind(state, 'B')
        refresh(state, NOW + 1_000, D('-0.018'))
        evaluate(state, NOW + 1_000)
        refresh(state, NOW + 16_000, D('-0.018'))
        evaluate(state, NOW + 16_000)
        self.assertEqual(state.recovery_started_ms, NOW + 16_000)

    def test_recovery_restart_with_long_gap_does_not_rearm(self):
        state = ready(4)
        self.assert_kind(state, 'B')
        for step in (1_000, 11_000, 21_000):
            refresh(state, NOW + step, D('-0.018'))
            evaluate(state, NOW + step)
        state = from_dict(to_dict(state))
        refresh(state, NOW + 81_000, D('-0.018'))
        evaluate(state, NOW + 81_000)
        self.assertEqual(state.recovery_started_ms, NOW + 81_000)
        self.assertIsNotNone(state.b_epoch)

    def test_recovery_unconfirmed_interval_breaks_continuity(self):
        state = ready(4)
        self.assert_kind(state, 'B')
        refresh(state, NOW + 1_000, D('-0.018'))
        evaluate(state, NOW + 1_000)
        state.interval_hours = None
        evaluate(state, NOW + 2_000)
        self.assertIsNone(state.recovery_started_ms)
        state.interval_hours = 4
        refresh(state, NOW + 3_000, D('-0.018'))
        evaluate(state, NOW + 3_000)
        self.assertEqual(state.recovery_started_ms, NOW + 3_000)

    def test_duplicate_funding_sample_does_not_advance_recovery(self):
        state = ready(4)
        self.assert_kind(state, 'B')
        refresh(state, NOW + 1_000, D('-0.018'))
        evaluate(state, NOW + 1_000)
        for step in range(2_000, 15_000, 1_000):
            evaluate(state, NOW + step)
        self.assertEqual(state.recovery_last_ms, NOW + 1_000)
        self.assertIsNotNone(state.b_epoch)

    def test_b_price_dip_does_not_reset_sent(self):
        state = ready(4)
        self.assert_kind(state, 'B')
        state.price = D('2')
        evaluate(state, NOW)
        state.price = D('3')
        self.assertEqual(evaluate(state, NOW), [])

    def test_interval_four_to_eight_does_not_duplicate_b(self):
        state = ready(4)
        self.assert_kind(state, 'B')
        state.interval_hours = 8
        self.assertEqual(evaluate(state, NOW), [])

    def test_legacy_unknown_classification_not_runtime_diagnostic(self):
        data = to_dict(ready())
        data['classification'] = 'UNKNOWN'
        state = from_dict(data)
        self.assertEqual(problems(state, NOW), {})
        self.assert_kind(state, 'A')

    def test_trading_halt_prevents_events(self):
        state = ready()
        state.trading = False
        self.assertEqual(evaluate(state, NOW), [])
        self.assertIsNone(state.a_epoch)
        self.assertEqual(problems(state, NOW), {})

    def test_legacy_market_cache_age_not_runtime_dependency(self):
        data = to_dict(ready())
        data['cg_checked_ms'] = NOW - 99 * DAY_MS
        self.assert_kind(from_dict(data), 'A')

    def test_floor_expiry_and_estimated_lifetime(self):
        state = ready(4)
        state.floor_ms = NOW - 300_001
        self.assertEqual(evaluate(state, NOW), [])
        state.floor_estimated = True
        self.assert_kind(state, 'B')
        state = ready(4)
        state.floor_estimated = True
        state.floor_ms = NOW - DAY_MS - 1
        self.assertEqual(evaluate(state, NOW), [])
        self.assertIn('FLOOR_UNKNOWN', problems(state, NOW))

    def test_last_verified_official_fallback_valid_24_hours(self):
        state = ready(4)
        state.floor_source = 'LAST_VERIFIED_OFFICIAL'
        state.floor_estimated = False
        state.floor_ms = NOW - DAY_MS
        event = self.assert_kind(state, 'B')
        self.assertIn('공식 하한: -2%', event.message)
        state = ready(4)
        state.floor_source = 'LAST_VERIFIED_OFFICIAL'
        state.floor_estimated = False
        state.floor_ms = NOW - DAY_MS - 1
        self.assertEqual(evaluate(state, NOW), [])
        self.assertIn('FLOOR_UNKNOWN', problems(state, NOW))

    def test_incomplete_current_candle_excluded(self):
        state = ready()
        current_open = NOW // FOUR_HOURS_MS * FOUR_HOURS_MS
        state.candles.append(Candle(current_open, current_open + FOUR_HOURS_MS - 1, D('0.01')))
        self.assertEqual(minimum_close(state, NOW), D('1'))
        self.assertEqual(minimum_close(state, NOW + 1), D('1'))

    def test_missing_candle_prevents_partial_history_signal(self):
        state = ready()
        del state.candles[100]
        self.assertEqual(evaluate(state, NOW), [])
        self.assertIn('PRICE_HISTORY_UNKNOWN', problems(state, NOW))

    def test_new_closed_candle_required_at_boundary(self):
        state = ready()
        self.assertEqual(minimum_close(state, NOW), D('1'))
        boundary = NOW // FOUR_HOURS_MS * FOUR_HOURS_MS + FOUR_HOURS_MS
        self.assertIsNone(minimum_close(state, boundary))
        fill_history(state, boundary)
        self.assertEqual(minimum_close(state, boundary), D('1'))

    def test_invalid_numbers_and_future_timestamps_block(self):
        for name, invalid in (('price', D('NaN')), ('price', D('Infinity')),
                              ('price', D('0')), ('price', 3.0),
                              ('funding', D('NaN')), ('funding', '-0.02')):
            with self.subTest(name=name, invalid=invalid):
                state = ready()
                setattr(state, name, invalid)
                self.assertEqual(evaluate(state, NOW), [])
        for name in ('price_ms', 'funding_ms', 'interval_ms'):
            state = ready()
            setattr(state, name, NOW + 1)
            self.assertEqual(evaluate(state, NOW), [])

    def test_invalid_candle_close_diagnosed(self):
        state = ready()
        state.candles[0] = replace(state.candles[0], close=D('NaN'))
        self.assertEqual(evaluate(state, NOW), [])
        self.assertIn('PRICE_HISTORY_UNKNOWN', problems(state, NOW))

    def test_history_not_required_during_listing_grace_period(self):
        state = ready(onboard=NOW - 168 * HOUR_MS + 1)
        state.history_ready = False
        state.candles = []
        self.assertNotIn('PRICE_HISTORY_UNKNOWN', problems(state, NOW))
        state.onboard_ms -= 1
        self.assertIn('PRICE_HISTORY_UNKNOWN', problems(state, NOW))

    def test_history_diagnostic_requires_confirmed_mature_listing(self):
        state = ready()
        state.history_ready = False
        state.candles = []
        self.assertIn('PRICE_HISTORY_UNKNOWN', problems(state, NOW))
        state.onboard_ms = 0
        self.assertNotIn('PRICE_HISTORY_UNKNOWN', problems(state, NOW))

    def test_json_preserves_decimal_text_and_official_fallback(self):
        state = ready(4)
        state.last_official_floor = D('-0.0200000000000000000000000001')
        state.last_official_floor_ms = NOW
        state.last_official_floor_source = 'fundingInfo'
        data = to_dict(state)
        self.assertEqual(data['last_official_floor'], '-0.0200000000000000000000000001')
        restored = from_dict(json.loads(json.dumps(data)))
        self.assertEqual(restored.last_official_floor, state.last_official_floor)
        self.assertEqual(restored.last_official_floor_source, 'fundingInfo')
        data['floor'] = 'NaN'
        self.assertIsNone(from_dict(data).floor)

    def test_new_listing_uuid_distinguishes_equal_epoch_timestamps(self):
        first = self.assert_kind(ready(), 'A')
        second = self.assert_kind(ready(), 'A')
        self.assertNotEqual(first.id, second.id)


    def test_slots_roundtrip_preserves_event_and_decimal_state(self):
        state = ready(4)
        event = self.assert_kind(state, 'B')
        restored = from_dict(json.loads(json.dumps(to_dict(state))))
        self.assertEqual(to_dict(state), to_dict(restored))
        self.assertTrue(event.id.endswith(restored.b_epoch))
        self.assertEqual(evaluate(restored, NOW), [])
        self.assertFalse(hasattr(restored, '__dict__'))
        self.assertFalse(hasattr(restored.candles[0], '__dict__'))

    def test_slots_replace_works_for_candles_and_state(self):
        state = ready()
        changed = replace(state, price=D('2.999'))
        self.assertEqual(evaluate(changed, NOW), [])
        self.assertEqual(state.price, D('3'))
        self.assertEqual(replace(state.candles[0], close=D('2')).close, D('2'))

    def test_unknown_listing_cannot_emit_even_with_complete_history(self):
        state = ready()
        state.onboard_ms = 0
        state.listing_status = 'RELISTING_UNKNOWN'
        self.assertEqual(evaluate(state, NOW), [])
        self.assertIn('LISTING_UNKNOWN', problems(state, NOW))


if __name__ == '__main__':
    unittest.main()
