"""Small asynchronous HTTP clients with conservative shared rate budgets."""
import asyncio
from collections import deque
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import time
import json
from decimal import Decimal

import aiohttp


class ApiFault(RuntimeError):
    def __init__(self, status=0, delay=10, reason='API unavailable'):
        super().__init__(reason)  # Never include request URLs, tokens or bodies.
        self.status = status
        self.delay = delay


def retry_delay(value, default=60):
    try:
        return max(1, float(value))
    except (TypeError, ValueError):
        try:
            return max(1, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return default


class WindowBudget:
    def __init__(self, capacity, seconds, clock=time.monotonic, sleep=asyncio.sleep):
        self.capacity, self.seconds = capacity, seconds
        self.clock, self.sleep = clock, sleep
        self.items = deque()
        self.lock = asyncio.Lock()

    async def take(self, weight=1):
        if not 0 < weight <= self.capacity:
            raise ValueError('Invalid request weight')
        async with self.lock:
            while True:
                now = self.clock()
                while self.items and self.items[0][0] <= now - self.seconds:
                    self.items.popleft()
                if sum(w for _, w in self.items) + weight <= self.capacity:
                    self.items.append((now, weight))
                    return
                await self.sleep(max(.01, self.items[0][0] + self.seconds - now))


async def json_request(session, method, url, *, params=None, payload=None, headers=None, timeout=20):
    try:
        async with session.request(method, url, params=params, json=payload, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status in (403, 451):
                raise ApiFault(response.status, 1800, 'Access blocked; 30 minute cooldown')
            if response.status in (429, 418):
                delay = retry_delay(response.headers.get('Retry-After'))
                try:
                    body = await response.json(content_type=None)
                    if isinstance(body, dict):
                        delay = max(delay, retry_delay((body.get('parameters') or {}).get('retry_after'), default=0))
                except (ValueError, aiohttp.ClientError):
                    pass
                raise ApiFault(response.status, delay, 'Rate limited')
            if response.status != 200:
                raise ApiFault(response.status, 60, 'HTTP request failed')
            # Binance's actual IP consumption may include other clients on this IP.
            used = int(response.headers.get('X-MBX-USED-WEIGHT-1M', '0'))
            try:
                value = await response.json(content_type=None, loads=lambda text: json.loads(text, parse_float=Decimal))
            except (ValueError, aiohttp.ClientError):
                raise ApiFault(0, 60, 'Malformed API JSON') from None
            return value, used
    except (aiohttp.ClientError, asyncio.TimeoutError):
        raise ApiFault(0, 10, 'Network request failed') from None


class Binance:
    def __init__(self, session):
        self.session = session
        self.weights = WindowBudget(1000, 60)
        self.funding = WindowBudget(400, 300)  # shared fundingInfo + fundingRate
        self.blocked_until = 0.0

    async def wait_cooldown(self):
        while self.blocked_until > time.monotonic():
            await asyncio.sleep(self.blocked_until - time.monotonic())

    async def get(self, path, params=None, weight=1):
        await self.wait_cooldown()
        await self.weights.take(weight)
        if path in ('/fundingInfo', '/fundingRate'):
            await self.funding.take()
        await self.wait_cooldown()
        try:
            data, used = await json_request(self.session, 'GET', 'https://fapi.binance.com/fapi/v1' + path,
                                           params=params)
            if used >= 1000:
                self.blocked_until = time.monotonic() + 60
            return data
        except ApiFault as exc:
            self.blocked_until = time.monotonic() + exc.delay
            raise

    async def prices(self):
        await self.wait_cooldown()
        await self.weights.take(2)
        await self.wait_cooldown()
        try:
            data, used = await json_request(self.session, 'GET',
                                           'https://fapi.binance.com/fapi/v2/ticker/price')
            if used >= 1000:
                self.blocked_until = time.monotonic() + 60
            return data
        except ApiFault as exc:
            self.blocked_until = time.monotonic() + exc.delay
            raise


class CoinGecko:
    def __init__(self, session, key, store, monthly_limit=6000):
        self.session, self.key, self.store = session, key, store
        self.monthly_limit = monthly_limit
        self.gate = WindowBudget(20, 60)
        self.lock = asyncio.Lock()
        self.next_request = 0.0
        self.metadata_retry_at = 0.0

    async def get(self, path, params=None):
        from storage import Backpressure
        metadata = path.startswith('/coins/') and path != '/coins/markets'
        if metadata and time.monotonic() < self.metadata_retry_at:
            raise Backpressure('Metadata budget cooldown')
        async with self.lock:
            await asyncio.sleep(max(0, self.next_request - time.monotonic()))
            await self.gate.take()
            limit = self.monthly_limit - min(1000, self.monthly_limit // 4) if metadata else self.monthly_limit
            try:
                await self.store.reserve_cg_call(int(time.time() * 1000), limit)
            except Backpressure:
                if metadata:
                    self.metadata_retry_at = time.monotonic() + 3600
                raise
            self.next_request = time.monotonic() + 3.1
            try:
                value, _ = await json_request(self.session, 'GET',
                    'https://api.coingecko.com/api/v3' + path, params=params,
                    headers={'x-cg-demo-api-key': self.key})
                return value
            except ApiFault as exc:
                self.next_request = time.monotonic() + max(60, exc.delay)
                raise


class Telegram:
    def __init__(self, session, token, chat_id):
        self.session = session
        self.base = 'https://api.telegram.org/bot' + token
        self.chat_id = str(chat_id)

    async def call(self, method, payload, timeout=20):
        result, _ = await json_request(self.session, 'POST', self.base + '/' + method,
                                       payload=payload, timeout=timeout)
        if not isinstance(result, dict) or not result.get('ok'):
            delay = (result.get('parameters') or {}).get('retry_after', 60) if isinstance(result, dict) else 60
            raise ApiFault(0, delay, 'Telegram operation rejected')
        return result.get('result')

    async def send(self, text):
        await self.call('sendMessage', {'chat_id': self.chat_id, 'text': text,
                                        'link_preview_options': {'is_disabled': True}})

    async def updates(self, offset):
        return await self.call('getUpdates', {'offset': offset, 'timeout': 30,
                                              'allowed_updates': ['message']}, timeout=40)


def delayed_message(row, now_ms):
    text = row['body']
    if row.get('attempts', 0) or now_ms - row['detected_ms'] >= 60_000:
        text += '\n지연 발송: 장애 또는 전송 대기 후 재전송. 원래 감지시각은 위 메시지에 표시됩니다.'
    return text
