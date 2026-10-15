import unittest
from rules import DAY_MS, HOUR_MS, MARGIN_FORMULA_SYMBOLS, resolve_funding

NOW = 100 * DAY_MS


def adjustment(interval=4, floor="-0.03"):
    return [{"symbol": "LINKUSDT", "fundingIntervalHours": interval,
             "adjustedFundingRateFloor": floor}]


def history(hours=4, rates=("-0.004", "-0.01", "-0.005")):
    return [{"symbol": "LINKUSDT", "fundingTime": NOW - (2 - i) * hours * HOUR_MS,
             "fundingRate": rate} for i, rate in enumerate(rates)]


class FundingRulesTests(unittest.TestCase):
    def test_adjusted_floor_priority_and_exact_decimal(self):
        result = resolve_funding("LINKUSDT", adjustment(4, "-0.03000000"), history(), [], NOW)
        self.assertEqual(result["floor"], "-0.03000000")
        self.assertFalse(result["floor_estimated"])
        self.assertEqual(result["interval"], 4)
        self.assertEqual(result["last_official_floor_ms"], NOW)

    def test_all_official_adjusted_supported_intervals(self):
        for interval in (1, 4, 8):
            with self.subTest(interval=interval):
                result = resolve_funding("LINKUSDT", adjustment(interval), [], [], NOW)
                self.assertEqual(result["interval"], interval)

    def test_ordinary_default_floor_and_interval(self):
        result = resolve_funding("ORDUSDT", [], [], [], NOW)
        self.assertEqual(result["interval"], 8)
        self.assertEqual(result["floor"], "-0.02")
        self.assertEqual(result["floor_source"], "OFFICIAL_DEFAULT_MINUS_2_PERCENT")

    def test_formula_exception_has_no_guessed_floor(self):
        for symbol in MARGIN_FORMULA_SYMBOLS:
            with self.subTest(symbol=symbol):
                self.assertIsNone(resolve_funding(symbol, [], [], [], NOW)["floor"])

    def test_exception_list_includes_current_additional_symbols(self):
        self.assertTrue({"XMRUSDT", "DOTUSDT", "UNIUSDT", "AAVEUSDT"}.issubset(MARGIN_FORMULA_SYMBOLS))

    def test_last_verified_official_precedes_observed(self):
        previous = {"last_official_floor": "-0.015", "last_official_floor_ms": NOW - HOUR_MS,
                    "last_official_floor_source": "OFFICIAL_ADJUSTED"}
        result = resolve_funding("LINKUSDT", [], history(), [], NOW, previous)
        self.assertEqual(result["floor"], "-0.015")
        self.assertEqual(result["floor_ms"], NOW - HOUR_MS)
        self.assertEqual(result["last_official_floor_ms"], NOW - HOUR_MS)
        self.assertFalse(result["floor_estimated"])

    def test_stale_last_verified_floor_does_not_rejuvenate(self):
        previous = {"last_official_floor": "-0.03", "last_official_floor_ms": NOW - DAY_MS}
        result = resolve_funding("LINKUSDT", [], history(), [], NOW, previous)
        self.assertEqual(result["floor"], "-0.01")
        self.assertTrue(result["floor_estimated"])
        self.assertEqual(result["last_official_floor_ms"], NOW - DAY_MS)

    def test_30_day_observed_min_excludes_old_future_other_symbol(self):
        rows = history() + [
            {"symbol": "LINKUSDT", "fundingTime": NOW - 30 * DAY_MS - 1, "fundingRate": "-0.5"},
            {"symbol": "LINKUSDT", "fundingTime": NOW + 1, "fundingRate": "-0.4"},
            {"symbol": "WRONGUSDT", "fundingTime": NOW, "fundingRate": "-0.3"},
            {"symbol": "LINKUSDT", "fundingTime": NOW, "fundingRate": "-0.2", "rateType": "Special"}]
        result = resolve_funding("LINKUSDT", [], rows, [], NOW)
        self.assertEqual(result["floor"], "-0.01")
        self.assertTrue(result["floor_estimated"])

    def test_observation_includes_exact_30_day_boundary(self):
        rows = [{"symbol": "LINKUSDT", "fundingTime": NOW - 30 * DAY_MS, "fundingRate": "-0.025"}]
        self.assertEqual(resolve_funding("LINKUSDT", [], rows, [], NOW)["floor"], "-0.025")

    def test_no_valid_negative_history_is_unknown(self):
        for bad in (None, "0", "0.01", "NaN", "sNaN", "Infinity", "bad", True, -0.02):
            with self.subTest(rate=bad):
                rows = [{"fundingTime": NOW, "fundingRate": bad}]
                self.assertIsNone(resolve_funding("LINKUSDT", [], rows, [], NOW)["floor"])

    def test_invalid_adjusted_floor_never_assigns_ordinary_default(self):
        rows = [{"symbol": "ORDUSDT", "fundingIntervalHours": 4,
                 "adjustedFundingRateFloor": "NaN"}]
        result = resolve_funding("ORDUSDT", rows, [], [], NOW)
        self.assertIsNone(result["floor"])
        self.assertEqual(result["interval"], 4)

    def test_official_change_replaces_estimate(self):
        old = resolve_funding("LINKUSDT", [], history(), [], NOW)
        new = resolve_funding("LINKUSDT", adjustment(4, "-0.02"), history(), [], NOW + 10_000, old)
        self.assertEqual(new["floor"], "-0.02")
        self.assertFalse(new["floor_estimated"])

    def test_time_until_next_does_not_imply_one_hour(self):
        result = resolve_funding("LINKUSDT", None, [], [NOW + HOUR_MS], NOW)
        self.assertIsNone(result["interval"])

    def test_successive_next_times_verify_interval(self):
        result = resolve_funding("LINKUSDT", None, [],
                                 [NOW - HOUR_MS, NOW, NOW + HOUR_MS], NOW)
        self.assertEqual(result["interval"], 1)
        self.assertEqual(result["interval_source"], "OBSERVED_NEXT_SETTLEMENTS")

    def test_recent_regular_history_verifies_interval(self):
        result = resolve_funding("LINKUSDT", None, history(4), [], NOW)
        self.assertEqual(result["interval"], 4)

    def test_stale_settlement_history_does_not_verify_interval(self):
        result = resolve_funding("LINKUSDT", None, history(1), [], NOW + 2 * HOUR_MS)
        self.assertIsNone(result["interval"])

    def test_fresh_default8_conflict_holds_signals(self):
        result = resolve_funding("LINKUSDT", [], history(1), [], NOW)
        self.assertIsNone(result["interval"])
        self.assertEqual(result["interval_source"], "INTERVAL_UNKNOWN_CONFLICT")

    def test_valid_adjusted_interval_precedes_supplement(self):
        result = resolve_funding("LINKUSDT", adjustment(8), history(1), [], NOW)
        self.assertEqual(result["interval"], 8)

    def test_unsupported_or_invalid_official_interval_is_unknown(self):
        for value in (2, 12, True, "NaN", "sNaN", "1.5", "bad"):
            with self.subTest(value=value):
                result = resolve_funding("LINKUSDT", adjustment(value), history(1), [], NOW)
                self.assertIsNone(result["interval"])

    def test_conflicting_recent_supplements_are_unknown(self):
        result = resolve_funding("LINKUSDT", None, history(4),
                                 [NOW - HOUR_MS, NOW, NOW + HOUR_MS], NOW)
        self.assertIsNone(result["interval"])
        self.assertEqual(result["interval_source"], "INTERVAL_UNKNOWN_CONFLICT")

    def test_next_timestamps_are_bounded_and_deduplicated(self):
        result = resolve_funding("LINKUSDT", None, [],
                                 [NOW + i * HOUR_MS for i in range(-10, 2)] + [NOW], NOW)
        self.assertEqual(len(result["next_times"]), 4)

    def test_unavailable_info_never_guesses_default(self):
        result = resolve_funding("ORDUSDT", None, [], [], NOW)
        self.assertIsNone(result["interval"])
        self.assertIsNone(result["floor"])

    def test_symbol_keyed_mapping_supported(self):
        result = resolve_funding("LINKUSDT", {"LINKUSDT": adjustment()[0]}, [], [], NOW)
        self.assertEqual(result["floor"], "-0.03")


if __name__ == "__main__":
    unittest.main()
