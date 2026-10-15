"""Durable diagnostic incident suppression, independent of signal evaluation.

Use a snapshot clone when preparing an outbox transaction, then publish the
clone only after PostgreSQL commits. Ordinary healthy/error frames do not
change timestamps repeatedly, avoiding a database write for every WS frame.
"""
import asyncio

import aiohttp


WS_GRACE_MS = 60_000
STABLE_RECOVERY_MS = 30 * 60_000


def fault_code(exc):
    """Return a bounded, secret-free cause, never an exception URL or body."""
    explicit = getattr(exc, 'reason_code', None)
    if explicit in {'CLOSED', 'TIMEOUT', 'DATA', 'NETWORK', 'API', 'INTERNAL'}:
        return explicit
    status = getattr(exc, 'status', 0)
    if isinstance(status, int) and 100 <= status <= 599:
        return 'HTTP_' + str(status)
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return 'TIMEOUT'
    if isinstance(exc, (aiohttp.ServerDisconnectedError, ConnectionResetError,
                        BrokenPipeError, EOFError)):
        return 'CLOSED'
    if isinstance(exc, (ValueError, TypeError)):
        return 'DATA'
    if isinstance(exc, (aiohttp.ClientError, OSError)):
        return 'NETWORK'
    return 'INTERNAL'


class IncidentTracker:
    """One notification per cause until 30 minutes of uninterrupted health.

    ``observe`` accepts None for health and a safe display detail for failure.
    A failure's 60 second WS grace is continuous: receiving a healthy frame
    restarts that grace, but it never clears an already notified incident.
    Distinct causes must have distinct keys, e.g. WS:mark:HTTP_403.
    """
    def __init__(self, records=None, restart_ms=None):
        self.records = {}
        if not isinstance(records, dict):
            return
        for key, row in records.items():
            if not isinstance(key, str) or not isinstance(row, dict):
                continue
            try:
                first = int(row['first_ms'])
                active_since = row.get('active_since_ms')
                healthy_since = row.get('healthy_since_ms')
                notified = row.get('notified_ms')
                self.records[key] = {
                    'first_ms': first,
                    'active_since_ms': int(active_since) if active_since is not None else None,
                    'healthy_since_ms': int(healthy_since) if healthy_since is not None else None,
                    'notified_ms': int(notified) if notified is not None else None,
                    'detail': str(row.get('detail', 'Service unavailable'))[:200],
                    'is_ws': bool(row.get('is_ws', False)),
                }
                if restart_ms is not None:
                    restored = self.records[key]
                    # Offline time cannot establish uninterrupted recovery or
                    # a continuously observed new WS failure. Never clear the
                    # durable notification marker when restarting.
                    if restored['healthy_since_ms'] is not None:
                        restored['healthy_since_ms'] = int(restart_ms)
                    elif restored['active_since_ms'] is not None and restored['notified_ms'] is None:
                        restored['active_since_ms'] = int(restart_ms)
            except (KeyError, ValueError, TypeError, OverflowError):
                continue

    def observe(self, key, detail, now_ms, is_ws=False):
        now_ms = int(now_ms)
        row = self.records.get(key)
        if row is not None:
            healthy = row['healthy_since_ms']
            if healthy is not None and now_ms - healthy >= STABLE_RECOVERY_MS:
                del self.records[key]
                row = None
                retired = True
            else:
                retired = False
        else:
            retired = False
        if detail is None:
            if row is None:
                return retired
            if row['healthy_since_ms'] is None:
                row['healthy_since_ms'] = now_ms
                row['active_since_ms'] = None
                return True
            return False
        detail = str(detail)[:200]
        if row is None:
            self.records[key] = {
                'first_ms': now_ms, 'active_since_ms': now_ms,
                'healthy_since_ms': None, 'notified_ms': None,
                'detail': detail, 'is_ws': bool(is_ws),
            }
            return True
        changed = False
        if row['active_since_ms'] is None:
            row['active_since_ms'] = now_ms
            row['healthy_since_ms'] = None
            changed = True
        if row['detail'] != detail or row['is_ws'] != bool(is_ws):
            row['detail'] = detail
            row['is_ws'] = bool(is_ws)
            changed = True
        return changed

    def due(self, now_ms):
        return {
            key: row['detail'] for key, row in self.records.items()
            if row['active_since_ms'] is not None and row['notified_ms'] is None
            and int(now_ms) - row['active_since_ms'] >= (WS_GRACE_MS if row['is_ws'] else 0)
        }

    def mark_notified(self, keys, now_ms):
        changed = False
        for key in keys:
            row = self.records.get(key)
            if row is not None and row['notified_ms'] is None:
                row['notified_ms'] = int(now_ms)
                changed = True
        return changed

    def snapshot(self):
        return {key: dict(row) for key, row in self.records.items()}
