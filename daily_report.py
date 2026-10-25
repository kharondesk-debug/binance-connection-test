"""Small, restart-safe daily diagnostics ledger; no network or database access.

The application commits a cloned ledger and closed-day reports atomically.
Only the current day's aggregates and currently active causes live in memory.
All timestamps are observed times, never estimated outage or recovery times.
"""
from datetime import date, datetime, timedelta, timezone
from copy import deepcopy
import json
import re
import base64
import zlib

DAY_MS = 86_400_000
KST_OFFSET_MS = 9 * 3_600_000
KST = timezone(timedelta(hours=9))
MAX_GROUPS = 128
EPOCH_DATE = date(1970, 1, 1)
_KIND = re.compile(r'^[A-Z][A-Z0-9_]*(?::[A-Z0-9_]+)*$')
_GAP_NOTES = {'관측 중단', '프로세스 재시작', 'DB 기록 공백', '원인 분류 한도', '과거 진단 전환'}


def day_key(ms):
    return (EPOCH_DATE + timedelta(days=(int(ms) + KST_OFFSET_MS) // DAY_MS)).isoformat()


def next_day(day):
    return (date.fromisoformat(day) + timedelta(days=1)).isoformat()


def start_ms(day):
    return (date.fromisoformat(day) - EPOCH_DATE).days * DAY_MS - KST_OFFSET_MS


def utf16_length(text):
    return len(text.encode('utf-16-le')) // 2


def _kind(value):
    value = str(value).upper()
    if len(value) > 96 or not _KIND.fullmatch(value):
        return 'OTHER:UNCLASSIFIED'
    return value


def _impact(value):
    return value if type(value) is int and value >= 0 else None


def _group(first=None):
    return {'events': 0, 'continued': 0, 'recovered': 0,
            'peak_impact': None, 'impact_unknown': False,
            'first_ms': first, 'last_ms': first, 'active': False, 'unknown': False}


class DailyLedger:
    """Pure aggregate state. Call roll(now) before observations on a new day.

    groups.events counts observed inactive-to-active transitions only.
    Continued incidents crossing midnight are explicitly separate. A restored
    active cause is unknown until another failure or successful recovery is
    actually observed. No offline time is assumed to be healthy.
    """
    def __init__(self, now_ms, snapshot=None, restarted=False):
        now_ms = int(now_ms)
        saved = snapshot if isinstance(snapshot, dict) else {}
        self.day = saved.get('day', day_key(now_ms))
        start_ms(self.day)  # Fail closed on a corrupt cursor.
        self.groups = deepcopy(saved.get('groups', {}))
        self.active = deepcopy(saved.get('active', {}))
        self.next_report_day = saved.get('next_report_day', self.day)
        start_ms(self.next_report_day)
        self.last_checkpoint_ms = int(saved.get('last_checkpoint_ms', now_ms))
        self.coverage_unknown = bool(saved.get('coverage_unknown', not saved and now_ms != start_ms(self.day)))
        self.coverage_notes = list(saved.get('coverage_notes', ['관측 중단'] if self.coverage_unknown else []))[:4]
        self.legacy_count = int(saved.get('legacy_count', 0))
        self.legacy_first_ms = saved.get('legacy_first_ms')
        self.legacy_last_ms = saved.get('legacy_last_ms')
        self.legacy_seen_total = int(saved.get('legacy_seen_total', 0))
        self._restart_ms = now_ms if restarted else None
        if len(self.groups) > MAX_GROUPS or len(self.active) > MAX_GROUPS:
            raise ValueError('Daily diagnostic cause budget exceeded')
        if restarted:
            self.mark_gap(now_ms, '프로세스 재시작')
            for key, item in self.active.items():
                item['known'] = False
                if key in self.groups:
                    self.groups[key]['unknown'] = True

    def snapshot(self):
        return deepcopy({
            'day': self.day, 'groups': self.groups, 'active': self.active,
            'next_report_day': self.next_report_day,
            'last_checkpoint_ms': self.last_checkpoint_ms,
            'coverage_unknown': self.coverage_unknown,
            'coverage_notes': self.coverage_notes,
            'legacy_count': self.legacy_count,
            'legacy_first_ms': self.legacy_first_ms,
            'legacy_last_ms': self.legacy_last_ms,
            'legacy_seen_total': self.legacy_seen_total,
        })

    def _same_day(self, now_ms):
        if day_key(now_ms) != self.day:
            raise ValueError('Call roll() before observing a different KST day')

    def _bounded_kind(self, kind):
        kind = _kind(kind)
        if kind not in self.groups and len(self.groups) >= MAX_GROUPS - 1:
            self.mark_gap(start_ms(self.day), '원인 분류 한도')
            return 'OTHER:OVERFLOW'
        return kind

    def observe(self, kind, impact, now_ms):
        self._same_day(now_ms)
        now_ms, impact = int(now_ms), _impact(impact)
        kind = self._bounded_kind(kind)
        before = (deepcopy(self.groups.get(kind)), deepcopy(self.active.get(kind)))
        item = self.active.get(kind)
        group = self.groups.setdefault(kind, _group(now_ms))
        if item is None:
            group['events'] += 1
            item = {'first_ms': now_ms, 'last_ms': now_ms, 'impact': impact, 'known': True}
            self.active[kind] = item
        else:
            item.update(last_ms=max(now_ms, item['last_ms']), impact=impact, known=True)
        group['first_ms'] = now_ms if group['first_ms'] is None else min(group['first_ms'], now_ms)
        group['last_ms'] = now_ms if group['last_ms'] is None else max(group['last_ms'], now_ms)
        if impact is None:
            group['impact_unknown'] = True
        else:
            group['peak_impact'] = max(group['peak_impact'] or 0, impact)
        group.update(active=True, unknown=False)
        # Last confirmed failure is touched in memory, but is flushed by the
        # application's minute heartbeat rather than every error observation.
        structural = lambda row: {k: v for k, v in (row or {}).items() if k != 'last_ms'}
        return (structural(before[0]), structural(before[1])) != (structural(group), structural(item))

    def resolve(self, kind, now_ms):
        self._same_day(now_ms)
        kind = _kind(kind)
        item = self.active.pop(kind, None)
        if item is None:
            return False
        group = self.groups.setdefault(kind, _group())
        group['recovered'] += 1
        group.update(active=False, unknown=False)
        group['recovered_ms'] = int(now_ms)
        return True

    def mark_gap(self, now_ms, reason='관측 중단'):
        reason = reason if reason in _GAP_NOTES else '관측 중단'
        changed = not self.coverage_unknown or reason not in self.coverage_notes
        self.coverage_unknown = True
        if reason not in self.coverage_notes and len(self.coverage_notes) < 4:
            self.coverage_notes.append(reason)
        return changed

    def checkpoint(self, now_ms):
        self._same_day(now_ms)
        now_ms = int(now_ms)
        if self.last_checkpoint_ms == now_ms:
            return False
        self.last_checkpoint_ms = now_ms
        return True

    def add_legacy(self, count, first_ms, last_ms, now_ms):
        """Record migrated message records, never infer incident counts."""
        self._same_day(now_ms)
        if type(count) is not int or count <= 0:
            return False
        self.legacy_count += count
        if first_ms is not None:
            value = int(first_ms)
            self.legacy_first_ms = value if self.legacy_first_ms is None else min(value, self.legacy_first_ms)
        if last_ms is not None:
            value = int(last_ms)
            self.legacy_last_ms = value if self.legacy_last_ms is None else max(value, self.legacy_last_ms)
        self.mark_gap(now_ms, '과거 진단 전환')
        return True

    def active_count(self):
        """Unresolved causes includes restored causes whose health is unknown."""
        return len(self.active)

    def day_payload(self):
        return deepcopy({
            'day': self.day, 'groups': self.groups,
            'coverage_unknown': self.coverage_unknown,
            'coverage_notes': self.coverage_notes,
            'legacy_count': self.legacy_count,
            'legacy_first_ms': self.legacy_first_ms,
            'legacy_last_ms': self.legacy_last_ms,
            'unresolved': self.active_count(),
        })

    def roll(self, now_ms):
        target = day_key(now_ms)
        if target == self.day:
            return []
        if target < self.day:
            self.mark_gap(now_ms)
            return []
        closed = (self.day, self.day_payload())
        skipped = next_day(self.day) != target
        restart_gap = (self._restart_ms is not None and day_key(self._restart_ms) == target
                       and self._restart_ms > start_ms(target))
        checkpoint_gap = int(now_ms) - self.last_checkpoint_ms > 120_000
        self.day = target
        self.groups = {}
        self.coverage_unknown = bool(skipped or restart_gap or checkpoint_gap)
        self.coverage_notes = (['프로세스 재시작'] if restart_gap else ['관측 중단']) if self.coverage_unknown else []
        self._restart_ms = None
        self.legacy_count = 0
        self.legacy_first_ms = self.legacy_last_ms = None
        for kind, active in self.active.items():
            group = _group()
            group.update(continued=1, active=True, unknown=not active.get('known', False))
            if active.get('impact') is None:
                group['impact_unknown'] = True
            else:
                group['peak_impact'] = active['impact']
            self.groups[kind] = group
        # Cursor is deliberately unchanged; the application advances it only in
        # the transaction registering each report.
        return [closed]


def _clock(ms):
    return '확인 불가' if ms is None else datetime.fromtimestamp(int(ms) / 1000, KST).strftime('%H:%M:%S')


def _full_clock(ms):
    return '확인 불가' if ms is None else datetime.fromtimestamp(int(ms) / 1000, KST).strftime('%m-%d %H:%M:%S')


def _status(group):
    if group.get('unknown'):
        return '확인 불가'
    return '미해결' if group.get('active') else '복구 완료'


def _metrics(group, compact=False):
    peak = group.get('peak_impact')
    impact = '확인 불가' if peak is None else str(peak)
    if group.get('impact_unknown') and peak is not None:
        impact += '+확인 불가'
    first, last = _clock(group.get('first_ms')), _clock(group.get('last_ms'))
    if compact:
        return (f"사건{group.get('events', 0)}/지속{group.get('continued', 0)} "
                f"영향최대{impact} {first}~{last} {_status(group)}")
    return (f"발생 사건: {group.get('events', 0)}회 · 전일 지속: {group.get('continued', 0)}회\n"
            f"최대 동시 영향: {impact}개\n"
            f"최초/마지막 관측: {first} / {last} KST · {_status(group)}")


def _health_lines(health):
    health = health or {}
    def value(key):
        val = health.get(key)
        return '확인 불가' if val is None else str(val)
    return ('📡 현재 감시 상태\n'
            f"감시 준비: {value('ready')} / {value('approved')}\n"
            f"가격 이력 준비: {value('history')} / {value('approved')}\n"
            f"시그널·승인 미전송: {value('pending')}건\n"
            f"현재 미해결 오류: {value('unresolved')}건")


def render_report(day, payload, generated_ms, health=None):
    """Render one report at <=3800 UTF-16 units without truncating incidents.

    Detailed payload is retained with the durable report. Compact layout groups
    causes sharing identical metrics, but preserves each cause's exact values.
    """
    start_ms(day)
    sent = datetime.fromtimestamp(int(generated_ms) / 1000, KST).strftime('%Y-%m-%d %H:%M:%S')
    head = f"📋 일일 오류 보고서\n집계일: {day} (KST)\n생성: {sent} KST\n"
    health = dict(health or {})
    tail = '\n' + _health_lines(health)
    if payload is None:
        return head + '\n관측 기록 없음: 발생 사건·영향·복구 상태 확인 불가.\n관측 중단 날짜의 지연 보고입니다.\n' + tail
    groups = payload.get('groups', {})
    total = sum(int(row.get('events', 0)) for row in groups.values())
    continued = sum(int(row.get('continued', 0)) for row in groups.values())
    summary = f"\n오류 종류: {len(groups)}개 · 신규 사건: {total}회 · 전일 지속: {continued}회\n"
    notes = []
    if payload.get('coverage_unknown'):
        notes.append('⚠ 기록 정확성 한계: 관측·DB 기록 공백이 있어 누락 여부 확인 불가.')
    if payload.get('legacy_count', 0):
        notes.append(f"기존 미전송 진단 {payload['legacy_count']}개 전환. "
                     f"전체 보관분 원래 관측시각 범위: {_full_clock(payload.get('legacy_first_ms'))}~{_full_clock(payload.get('legacy_last_ms'))} KST. "
                     '메시지 수이며 실제 장애 사건 수는 확인 불가.')
    note = '\n' + '\n'.join(notes) if notes else ''
    if not groups:
        empty = ('관측된 오류 없음. 전체 기간 오류 여부는 확인 불가.'
                 if payload.get('coverage_unknown') else '오늘 발생한 오류 없음')
        return head + '\n' + empty + note + tail
    detail = '\n\n'.join(kind + '\n' + _metrics(row) for kind, row in sorted(groups.items()))
    text = head + summary + detail + note + tail
    if utf16_length(text) <= 3800:
        return text
    detail = '\n'.join(kind + ': ' + _metrics(row, True) for kind, row in sorted(groups.items()))
    text = head + summary + '각 행: 신규 사건/전일 지속, 최대 동시 영향, 최초~마지막 KST, 복구 상태\n' + detail + note + tail
    if utf16_length(text) <= 3800:
        return text
    # Equal metrics share one legend row. This is lossless, including recovery
    # state, count, impact and times for every listed cause.
    equal = {}
    for kind, row in sorted(groups.items()):
        equal.setdefault(_metrics(row, True), []).append(kind)
    detail = '\n'.join(','.join(kinds) + '\n' + metrics for metrics, kinds in equal.items())
    text = head + summary + detail + note + tail
    if utf16_length(text) <= 3800:
        return text
    # Compact notation keeps every metric while replacing repeated runtime
    # taxonomy words with a fixed, readable legend.
    aliases = {
        'SERVICE': 'S', 'CLOCK': 'CLK', 'EXCHANGE': 'EX', 'FUNDING_INFO': 'FI',
        'FUNDING_HISTORY': 'FH', 'LIVE_RECHECK': 'LR', 'LIVE_RECOVERY': 'LRC',
        'KLINES': 'KL', 'WS': 'W', 'MARK': 'M', 'PRICE': 'P',
        'TELEGRAM': 'TG', 'TELEGRAM_POLL': 'TP', 'DIAGNOSTICS': 'DG',
        'HEALTHCHECKS': 'HC', 'PRICE_STALE': 'PST', 'PRICE_HISTORY_UNKNOWN': 'PHU',
        'FUNDING_STALE': 'FST', 'PRICE_INVALID': 'PINV', 'FUNDING_INVALID': 'FINV',
        'INTERVAL_UNKNOWN': 'INT?', 'FLOOR_UNKNOWN': 'FLOOR?', 'LISTING_UNKNOWN': 'LIST?',
        'TIMEOUT': 'TMO', 'NETWORK': 'NET', 'CLOSED': 'CLS', 'INTERNAL': 'INT',
    }
    used = set()
    rows = []
    for kind, row in sorted(groups.items()):
        parts = []
        for token in kind.split(':'):
            if token.startswith('HTTP_') and token[5:].isdigit():
                parts.append(token[5:])
            else:
                alias = aliases.get(token, token)
                if alias != token:
                    used.add(token)
                parts.append(alias)
        peak = row.get('peak_impact')
        impact = '?' if peak is None else str(peak)
        if row.get('impact_unknown') and peak is not None:
            impact += '+?'
        first = _clock(row.get('first_ms')).replace(':', '') if row.get('first_ms') is not None else '?'
        last = _clock(row.get('last_ms')).replace(':', '') if row.get('last_ms') is not None else '?'
        times = first if first == last else first + '-' + last
        status = '?' if row.get('unknown') else ('A' if row.get('active') else 'R')
        rows.append('.'.join(parts) + f"|{row.get('events', 0)}/{row.get('continued', 0)}|{impact}|{times}|{status}")
    legend = ('행: 종류|신규/전일지속|최대동시영향|최초-마지막(HHMMSS,KST)|상태\n'
              'A=미해결 R=복구완료 ?=확인불가; 숫자 원인=HTTP상태\n'
              + '; '.join(aliases[key] + '=' + key for key in sorted(used)) + '\n')
    text = head + summary + legend + '\n'.join(rows) + note + tail
    if utf16_length(text) <= 3800:
        return text
    # Pathological overflow: retain the entire exact report payload inline
    # rather than silently omit types or pretend a partial list is complete.
    # This is not the normal runtime layout; the same plain payload stays in DB.
    exact = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    encoded = base64.b64encode(zlib.compress(exact, 9)).decode('ascii')
    text = (head + summary + '상세 유형·횟수·영향·시각·복구 상태가 매우 많아 무손실 압축했습니다.\n'
            '전체 상세는 PostgreSQL 일일 보고서 원장에 보존됩니다.\n'
            '압축 상세(UTF-8 JSON, zlib+base64):\n' + encoded + note + tail)
    if utf16_length(text) <= 3800:
        return text
    raise ValueError('Diagnostic payload exceeds bounded runtime taxonomy budget')
