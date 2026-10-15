"""Exact, deterministic funding signal rules; no network or storage side effects."""
from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import uuid4

HOUR_MS = 3_600_000
FOUR_HOURS_MS = 4 * HOUR_MS
DAY_MS = 24 * HOUR_MS
KST = timezone(timedelta(hours=9))


@dataclass(frozen=True)
class Candle:
    open_ms: int
    close_ms: int
    close: Decimal


@dataclass(frozen=True)
class Event:
    id: str
    symbol: str
    kind: str
    detected_ms: int
    message: str


@dataclass
class State:
    symbol: str
    base: str
    onboard_ms: int
    name: str = ''
    source_onboard_ms: int = 0
    listing_status: str = 'CONFIRMED'
    listing_evidence_ms: int = 0
    trading: bool = True
    candles: list[Candle] = field(default_factory=list)
    history_ready: bool = False
    price: Decimal | None = None
    price_ms: int = 0
    funding: Decimal | None = None
    funding_ms: int = 0
    next_funding_ms: int = 0
    next_times: list[int] = field(default_factory=list)
    interval_hours: int | None = None
    interval_ms: int = 0
    interval_source: str = ''
    floor: Decimal | None = None
    floor_ms: int = 0
    floor_source: str = ''
    floor_estimated: bool = False
    last_official_floor: Decimal | None = None
    last_official_floor_source: str = ''
    last_official_floor_ms: int = 0
    known_interval: int | None = None
    previous_interval: int | None = None
    a_epoch: str | None = None
    a_sent: bool = False
    b_epoch: str | None = None
    b_sent: bool = False
    b_floor: Decimal | None = None
    recovery_started_ms: int | None = None
    recovery_last_ms: int | None = None
    history_revision: int = 0
    history_min: Decimal | None = None
    history_min_close_ms: int = 0
    history_cache_revision: int = -1
    history_cache_count: int = -1
    history_cache_last_ms: int = 0
    history_cache_ready: bool = False
    history_cache_closed_ms: int = 0


DECIMAL_FIELDS = frozenset({
    'price', 'funding', 'floor', 'last_official_floor',
    'b_floor', 'history_min',
})


def _decimal(value: Any) -> Decimal | None:
    """Reject binary floats and non-finite API values instead of approximating them."""
    if value is None or isinstance(value, (bool, float)):
        return None
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _positive(value: Any) -> bool:
    return isinstance(value, Decimal) and value.is_finite() and value > 0


def _finite(value: Any) -> bool:
    return isinstance(value, Decimal) and value.is_finite()


def _fresh(stamp: int, now_ms: int, maximum: int, *, inclusive: bool = False) -> bool:
    if not isinstance(stamp, int) or isinstance(stamp, bool) or stamp <= 0:
        return False
    elapsed = now_ms - stamp
    return 0 <= elapsed <= maximum if inclusive else 0 <= elapsed < maximum


def _interval_valid(state: State, now_ms: int) -> bool:
    return (type(state.interval_hours) is int and state.interval_hours in (1, 4, 8)
            and _fresh(state.interval_ms, now_ms, 300_000, inclusive=True))


def _floor_valid(state: State, now_ms: int) -> bool:
    maximum = DAY_MS if (state.floor_estimated or state.floor_source == 'LAST_VERIFIED_OFFICIAL') else 300_000
    return (_finite(state.floor) and state.floor < 0
            and _fresh(state.floor_ms, now_ms, maximum, inclusive=True))


def minimum_close(state: State, now_ms: int) -> Decimal | None:
    """Use a cached low; rescan only at history changes or expiration of that low.

    Range membership uses a completed candle's close timestamp. A listing's
    initial, possibly partial, four-hour candle is included. Full coverage is
    checked so a failed historical download never silently changes the rule.
    """
    if not state.history_ready or state.onboard_ms <= 0 or not state.candles:
        return None
    lower = max(state.onboard_ms, now_ms - 90 * DAY_MS)
    expected_last = now_ms // FOUR_HOURS_MS * FOUR_HOURS_MS - 1
    last = state.candles[-1].close_ms if isinstance(state.candles[-1], Candle) else -1
    same = (state.history_cache_revision == state.history_revision
            and state.history_cache_count == len(state.candles)
            and state.history_cache_last_ms == last)
    if (same and state.history_cache_ready and _positive(state.history_min)
            and state.history_min_close_ms >= lower
            and state.history_cache_closed_ms == expected_last):
        return state.history_min
    # Newly completed candles must arrive before the next signal is evaluated.
    if same and state.history_cache_closed_ms != expected_last:
        return None
    valid: list[Candle] = []
    for candle in state.candles:
        if not isinstance(candle, Candle):
            return None
        if candle.close_ms < lower or candle.close_ms >= now_ms:
            continue
        if (not _positive(candle.close)
                or candle.open_ms % FOUR_HOURS_MS != 0
                or candle.close_ms != candle.open_ms + FOUR_HOURS_MS - 1):
            return None
        valid.append(candle)
    valid.sort(key=lambda candle: candle.close_ms)
    complete = (bool(valid) and valid[0].close_ms < lower + FOUR_HOURS_MS
                and valid[-1].close_ms == expected_last
                and all(right.close_ms - left.close_ms == FOUR_HOURS_MS
                        for left, right in zip(valid, valid[1:])))
    state.history_cache_revision = state.history_revision
    state.history_cache_count = len(state.candles)
    state.history_cache_last_ms = last
    state.history_cache_ready = complete
    state.history_cache_closed_ms = valid[-1].close_ms if valid else 0
    if not complete:
        state.history_min = None
        state.history_min_close_ms = 0
        return None
    minimum = min(valid, key=lambda candle: candle.close)
    state.history_min = minimum.close
    state.history_min_close_ms = minimum.close_ms
    return minimum.close


def problems(state: State, now_ms: int) -> dict[str, str]:
    """Diagnostic state keys; normal non-signal conditions are not failures.

    The caller persists diagnostic membership, batches newly appearing keys,
    and removes resolved keys. This function never sends messages.
    """
    result: dict[str, str] = {}
    if not state.trading:
        return result
    if state.onboard_ms <= 0:
        result['LISTING_UNKNOWN'] = '상장시각을 확인할 수 없습니다.'
    if not _interval_valid(state, now_ms):
        result['INTERVAL_UNKNOWN'] = '현재 정산 주기가 미확인 또는 300초를 초과해 갱신되지 않았습니다.'
    elif state.interval_hours in (4, 8) and not _floor_valid(state, now_ms):
        result['FLOOR_UNKNOWN'] = '공식 또는 추정 음수 하한이 미확인 또는 유효기간을 넘었습니다.'
    if not _finite(state.funding):
        result['FUNDING_INVALID'] = '실시간 예상 펀딩비가 유효하지 않습니다.'
    elif not _fresh(state.funding_ms, now_ms, 15_000):
        result['FUNDING_STALE'] = '예상 펀딩비가 15초 이상 갱신되지 않았습니다.'
    if not _positive(state.price):
        result['PRICE_INVALID'] = '현재 선물 체결가격이 유효한 양수가 아닙니다.'
    elif not _fresh(state.price_ms, now_ms, 120_000):
        result['PRICE_STALE'] = '실제 선물가격이 120초 이상 갱신되지 않았습니다.'
    history_expected = (state.onboard_ms > 0
                        and now_ms - state.onboard_ms >= 168 * HOUR_MS)
    if history_expected and minimum_close(state, now_ms) is None:
        result['PRICE_HISTORY_UNKNOWN'] = '최근 구간의 완성된 4시간봉 이력이 아직 준비되지 않았습니다.'
    return result


def _reset_b(state: State) -> None:
    state.b_epoch = None
    state.b_sent = False
    state.recovery_started_ms = None
    state.recovery_last_ms = None


def _observe_segments(state: State, now_ms: int) -> None:
    if not state.trading:
        state.recovery_started_ms = None
        state.recovery_last_ms = None
        return
    if not _interval_valid(state, now_ms):
        # Unknown data does not prove that a known one-hour segment ended.
        state.recovery_started_ms = None
        state.recovery_last_ms = None
        return
    current = state.interval_hours
    if current != state.known_interval:
        state.previous_interval = state.known_interval
        state.known_interval = current
        if current == 1:
            state.a_epoch = str(uuid4())
            state.a_sent = False
        else:
            state.a_epoch = None
            state.a_sent = False
    elif current == 1 and state.a_epoch is None:
        state.a_epoch = str(uuid4())
        state.a_sent = False
    if current == 1:
        _reset_b(state)
        return
    if not _floor_valid(state, now_ms):
        state.recovery_started_ms = None
        state.recovery_last_ms = None
        return
    if state.b_floor != state.floor:
        _reset_b(state)
        state.b_floor = state.floor
    if not _finite(state.funding) or not _fresh(state.funding_ms, now_ms, 15_000):
        state.recovery_started_ms = None
        state.recovery_last_ms = None
        return
    if state.funding <= state.floor:
        state.recovery_started_ms = None
        state.recovery_last_ms = None
        if state.b_epoch is None:
            state.b_epoch = str(uuid4())
            state.b_sent = False
        return
    recovery_level = state.floor + abs(state.floor) * Decimal('0.10')
    if state.b_epoch is None or state.funding < recovery_level:
        state.recovery_started_ms = None
        state.recovery_last_ms = None
        return
    observed_ms = state.funding_ms
    last_ms = state.recovery_last_ms
    # Distinct samples establish continuity. A stale gap or time reversal cannot
    # count as 60 seconds of recovery, including after a process restart.
    if (last_ms is None or observed_ms < last_ms
            or observed_ms - last_ms >= 15_000):
        state.recovery_started_ms = observed_ms
    if last_ms is not None and observed_ms == last_ms:
        return
    state.recovery_last_ms = observed_ms
    if (state.recovery_started_ms is not None
            and observed_ms - state.recovery_started_ms >= 60_000):
        _reset_b(state)


def _number(value: Decimal) -> str:
    return format(value, 'f').rstrip('0').rstrip('.') if '.' in format(value, 'f') else format(value, 'f')


def _when(milliseconds: int) -> str:
    if milliseconds <= 0:
        return '미확인'
    return datetime.fromtimestamp(milliseconds / 1000, KST).strftime('%Y-%m-%d %H:%M:%S KST')


def _message(state: State, kind: str, now_ms: int, low: Decimal) -> str:
    previous = f'{state.previous_interval}시간' if state.previous_interval else '미확인'
    lines = [
        f'[{kind}] {state.name or state.base} ({state.symbol})',
        f'현재 정산 주기: {state.interval_hours}시간 / 이전: {previous}',
        f'현재 예상 펀딩비: {_number(state.funding * 100)}%',
    ]
    if kind == 'B':
        qualifier = '추정 하한' if state.floor_estimated else '공식 하한'
        lines.append(f'{qualifier}: {_number(state.floor * 100)}% / 출처: {state.floor_source or "미확인"}')
    lines.extend([
        f'현재 선물 체결가격: {_number(state.price)} USDT',
        f'최근 90일 최저 4시간봉 종가: {_number(low)} USDT',
        f'상승배율: {_number(state.price / low)}배',
        f'감지 시각: {_when(now_ms)}',
        f'다음 정산 예정시각: {_when(state.next_funding_ms)}',
        '발생 이유: 가격 3배 이상이며 ' + (
            '1시간 정산의 예상 펀딩비가 음수입니다.' if kind == 'A'
            else f'{state.interval_hours}시간 정산의 예상 펀딩비가 음수 하한에 도달했습니다.'),
        f'https://www.binance.com/en/futures/{state.symbol}',
    ])
    return '\n'.join(lines)


def evaluate(state: State, now_ms: int) -> list[Event]:
    """Mutate bounded current event state and return newly detected messages.

    *_sent denotes durably queued once, rather than a Telegram HTTP success.
    The caller MUST atomically persist this State and these Events in one DB
    transaction and restore/reload State if the transaction fails.
    """
    _observe_segments(state, now_ms)
    if (not state.trading or state.onboard_ms <= 0
            or now_ms - state.onboard_ms < 168 * HOUR_MS
            or not _positive(state.price) or not _fresh(state.price_ms, now_ms, 120_000)
            or not _finite(state.funding) or not _fresh(state.funding_ms, now_ms, 15_000)
            or state.funding >= 0 or not _interval_valid(state, now_ms)):
        return []
    low = minimum_close(state, now_ms)
    if low is None or state.price < low * Decimal('3'):
        return []
    if state.interval_hours == 1 and state.a_epoch and not state.a_sent:
        state.a_sent = True
        event_id = f'{state.symbol}:A:{state.a_epoch}'
        return [Event(event_id, state.symbol, 'A', now_ms, _message(state, 'A', now_ms, low))]
    if (state.interval_hours in (4, 8) and _floor_valid(state, now_ms)
            and state.funding <= state.floor and state.b_epoch and not state.b_sent):
        state.b_sent = True
        event_id = f'{state.symbol}:B:{state.b_epoch}'
        return [Event(event_id, state.symbol, 'B', now_ms, _message(state, 'B', now_ms, low))]
    return []


def to_dict(state: State) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in fields(State):
        value = getattr(state, item.name)
        if item.name in DECIMAL_FIELDS:
            result[item.name] = str(value) if _finite(value) else None
        elif item.name == 'candles':
            result[item.name] = [{'open_ms': candle.open_ms, 'close_ms': candle.close_ms,
                                  'close': str(candle.close)} for candle in value]
        elif item.name == 'next_times':
            result[item.name] = value[-8:]
        else:
            result[item.name] = value
    return result


def from_dict(data: dict[str, Any]) -> State:
    known = {item.name for item in fields(State)}
    values = {key: value for key, value in data.items() if key in known}
    for key in DECIMAL_FIELDS & values.keys():
        values[key] = _decimal(values[key])
    values['candles'] = [Candle(int(item['open_ms']), int(item['close_ms']),
                               _decimal(item['close']) or Decimal('NaN'))
                         for item in values.get('candles', [])]
    values['next_times'] = [int(stamp) for stamp in values.get('next_times', [])][-8:]
    return State(**values)




