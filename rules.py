"""Public funding rules; financial numbers remain exact Decimal strings.

Sources: Binance Funding Rates FAQ 360033525031 and fundingInfo API.
No authenticated leverage-bracket endpoint or trading key is used.
"""
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
ALLOWED_INTERVALS = (1, 4, 8)
# The FAQ's maximum-leverage maintenance-margin formula applies to these
# symbols, so a universal -2% floor must never be assigned to them.
MARGIN_FORMULA_SYMBOLS = frozenset({
    "BTCUSDT", "ETHBTC", "ETHUSDT", "BNBUSDT", "XRPUSDT", "SOLUSDT",
    "DOGEUSDT", "TRXUSDT", "ADAUSDT", "LINKUSDT", "XLMUSDT", "BCHUSDT",
    "SUIUSDT", "AVAXUSDT", "LTCUSDT", "1000SHIBUSDT", "XMRUSDT",
    "DOTUSDT", "UNIUSDT", "AAVEUSDT",
})


def _negative(value):
    if isinstance(value, (bool, float)) or value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() and Decimal("-1") <= parsed < 0 else None


def _timestamp(value):
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def _interval(value):
    if isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return int(parsed) if parsed.is_finite() and parsed in ALLOWED_INTERVALS else None


def _adjustment(symbol, adjustments):
    """None means unavailable; an empty complete response means no adjustment."""
    if adjustments is None:
        return False, None
    if isinstance(adjustments, Mapping):
        if "symbol" in adjustments:
            return True, adjustments if adjustments.get("symbol") == symbol else None
        row = adjustments.get(symbol)
        return True, row if isinstance(row, Mapping) else ({ } if symbol in adjustments else None)
    if isinstance(adjustments, (list, tuple)):
        for row in adjustments:
            if isinstance(row, Mapping) and row.get("symbol") == symbol:
                return True, row
        return True, None
    return False, None


def _same_interval(times):
    if len(times) < 3:
        return None
    last = sorted(set(times))[-3:]
    if len(last) < 3:
        return None
    for hours in ALLOWED_INTERVALS:
        if all(abs((right - left) - hours * HOUR_MS) <= 1000
               for left, right in zip(last, last[1:])):
            return hours
    return None


def _observed_interval(history, next_times, now_ms):
    settled = sorted({time for row in history
                      if row.get("rateType", "Regular") == "Regular"
                      and (time := _timestamp(row.get("fundingTime"))) is not None
                      and time <= now_ms})
    historical = _same_interval(settled)
    if historical and now_ms - settled[-1] > historical * HOUR_MS + 15_000:
        historical = None
    next_values = sorted({time for value in next_times
                          if (time := _timestamp(value)) is not None})[-4:]
    projected = _same_interval(next_values)
    if projected and not (0 < next_values[-1] - now_ms <= projected * HOUR_MS + 15_000):
        projected = None
    if historical and projected and historical != projected:
        return None, "INTERVAL_UNKNOWN_CONFLICT", next_values
    if projected:
        return projected, "OBSERVED_NEXT_SETTLEMENTS", next_values
    if historical:
        return historical, "OBSERVED_CONFIRMED_SETTLEMENTS", next_values
    return None, "INTERVAL_UNKNOWN", next_values


def resolve_funding(symbol, adjustments, row_history, next_times, now_ms, previous=None):
    """Resolve the 1/4/8h interval and floor without guessing missing metadata.

    adjustments is the complete successful fundingInfo response (list or
    symbol-keyed dict), or None for unavailable data. row_history contains
    fundingRate records. next_times contains successive advertised settlement
    timestamps, never timestamps computed from the time remaining.
    previous is a persisted previous result. Retained official floors expire
    after 24h; their original verification timestamp is never refreshed.
    The caller also enforces its fundingInfo, price and mark-data age limits.
    """
    previous = previous if isinstance(previous, Mapping) else {}
    history = [row for row in (row_history or []) if isinstance(row, Mapping)
               and row.get("symbol", symbol) == symbol]
    observed, observed_source, retained_next = _observed_interval(
        history, next_times or [], now_ms)
    complete, adjusted = _adjustment(symbol, adjustments)
    interval, interval_source = None, "INTERVAL_UNKNOWN"
    if adjusted is not None:
        interval = _interval(adjusted.get("fundingIntervalHours"))
        if interval is not None:
            interval_source = "OFFICIAL_ADJUSTED"
        elif adjusted.get("fundingIntervalHours") is None:
            interval, interval_source = observed, observed_source
        # A stated unsupported/invalid official interval cannot be overwritten.
    elif complete:
        if observed is not None and observed != 8:
            interval_source = "INTERVAL_UNKNOWN_CONFLICT"
        elif observed_source == "INTERVAL_UNKNOWN_CONFLICT":
            interval_source = observed_source
        else:
            interval, interval_source = 8, "OFFICIAL_DEFAULT_8H"
    else:
        interval, interval_source = observed, observed_source

    floor, floor_source, floor_estimated, floor_ms = None, "FLOOR_UNKNOWN", False, 0
    last_floor = _negative(previous.get("last_official_floor"))
    last_ms = _timestamp(previous.get("last_official_floor_ms")) or 0
    last_source = previous.get("last_official_floor_source")
    adjusted_floor = _negative(adjusted.get("adjustedFundingRateFloor")) if adjusted is not None else None
    if adjusted_floor is not None:
        floor, floor_source, floor_ms = adjusted_floor, "OFFICIAL_ADJUSTED", now_ms
    elif complete and adjusted is None and symbol not in MARGIN_FORMULA_SYMBOLS:
        floor, floor_source, floor_ms = Decimal("-0.02"), "OFFICIAL_DEFAULT_MINUS_2_PERCENT", now_ms
    elif last_floor is not None and 0 <= now_ms - last_ms < DAY_MS:
        floor, floor_source, floor_ms = last_floor, "LAST_VERIFIED_OFFICIAL", last_ms
    else:
        negatives = [rate for row in history
                     if row.get("rateType", "Regular") == "Regular"
                     and (time := _timestamp(row.get("fundingTime"))) is not None
                     and now_ms - 30 * DAY_MS <= time <= now_ms
                     and (rate := _negative(row.get("fundingRate"))) is not None]
        if negatives:
            floor = min(negatives)
            floor_source, floor_estimated, floor_ms = "ESTIMATED_30D_OBSERVED_MIN", True, now_ms
    if floor_source in ("OFFICIAL_ADJUSTED", "OFFICIAL_DEFAULT_MINUS_2_PERCENT"):
        last_floor, last_ms, last_source = floor, now_ms, floor_source
    return {
        "interval": interval, "interval_source": interval_source,
        "floor": str(floor) if floor is not None else None,
        "floor_source": floor_source, "floor_estimated": floor_estimated,
        "floor_ms": floor_ms,
        "last_official_floor": str(last_floor) if last_floor is not None else None,
        "last_official_floor_ms": last_ms,
        "last_official_floor_source": last_source,
        "next_times": retained_next,
    }
