"""Offline incident durability and Telegram transport regression checks."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock

import aiohttp

from api import ApiFault, Binance, Telegram, delayed_message, json_request
from incidents import IncidentTracker, STABLE_RECOVERY_MS, fault_code


class IncidentTests(unittest.TestCase):
    def test_ws_transient_under_sixty_seconds_is_quiet(self):
        tracker = IncidentTracker()
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 0, is_ws=True)
        self.assertEqual(tracker.due(59_999), {})
        tracker.observe('WS:mark:CLOSED', None, 59_999)
        self.assertEqual(tracker.due(60_000), {})

    def test_sixty_second_boundary_notified_only_once(self):
        tracker = IncidentTracker()
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 100, is_ws=True)
        self.assertEqual(tracker.due(60_099), {})
        self.assertEqual(tracker.due(60_100), {'WS:mark:CLOSED': 'CLOSED'})
        tracker.mark_notified(tracker.due(60_100), 60_100)
        self.assertEqual(tracker.due(1_000_000), {})

    def test_repeated_error_frames_do_not_reset_grace_or_write(self):
        tracker = IncidentTracker()
        self.assertTrue(tracker.observe('WS:mark:TIMEOUT', 'TIMEOUT', 0, is_ws=True))
        for now in range(1000, 60_001, 1000):
            self.assertFalse(tracker.observe('WS:mark:TIMEOUT', 'TIMEOUT', now, is_ws=True))
        self.assertEqual(tracker.due(60_000), {'WS:mark:TIMEOUT': 'TIMEOUT'})

    def test_short_healthy_frame_then_same_failure_stays_notified(self):
        tracker = IncidentTracker()
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 0, is_ws=True)
        tracker.mark_notified(tracker.due(60_000), 60_000)
        tracker.observe('WS:mark:CLOSED', None, 60_010)
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 60_020, is_ws=True)
        self.assertEqual(tracker.due(900_000), {})

    def test_intermittent_healthy_frames_reset_continuous_grace(self):
        tracker = IncidentTracker()
        tracker.observe('WS:price:CLOSED', 'CLOSED', 0, is_ws=True)
        tracker.observe('WS:price:CLOSED', None, 59_000)
        tracker.observe('WS:price:CLOSED', 'CLOSED', 59_001, is_ws=True)
        self.assertEqual(tracker.due(60_000), {})
        self.assertEqual(tracker.due(119_001), {'WS:price:CLOSED': 'CLOSED'})

    def test_stable_recovery_exact_thirty_minute_boundary(self):
        tracker = IncidentTracker()
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 0, is_ws=True)
        tracker.mark_notified(tracker.due(60_000), 60_000)
        tracker.observe('WS:mark:CLOSED', None, 60_001)
        self.assertFalse(tracker.observe('WS:mark:CLOSED', None, 60_001 + STABLE_RECOVERY_MS - 1))
        self.assertTrue(tracker.observe('WS:mark:CLOSED', 'CLOSED', 60_001 + STABLE_RECOVERY_MS, is_ws=True))
        self.assertEqual(tracker.due(60_001 + STABLE_RECOVERY_MS + 59_999), {})
        self.assertEqual(len(tracker.due(60_001 + STABLE_RECOVERY_MS + 60_000)), 1)

    def test_recovery_interruption_restarts_thirty_minute_period(self):
        tracker = IncidentTracker()
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 0, is_ws=True)
        tracker.mark_notified(tracker.due(60_000), 60_000)
        tracker.observe('WS:mark:CLOSED', None, 100_000)
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 1_899_999, is_ws=True)
        tracker.observe('WS:mark:CLOSED', None, 1_900_000)
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 2_000_000, is_ws=True)
        self.assertEqual(tracker.due(3_000_000), {})

    def test_stable_health_retires_record_and_no_repeated_writes(self):
        tracker = IncidentTracker()
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 0, is_ws=True)
        self.assertTrue(tracker.observe('WS:mark:CLOSED', None, 1))
        self.assertFalse(tracker.observe('WS:mark:CLOSED', None, 2))
        self.assertTrue(tracker.observe('WS:mark:CLOSED', None, STABLE_RECOVERY_MS + 1))
        self.assertEqual(tracker.snapshot(), {})
        self.assertFalse(tracker.observe('WS:mark:CLOSED', None, STABLE_RECOVERY_MS + 2))

    def test_restart_preserves_notification_and_flapping_suppression(self):
        tracker = IncidentTracker()
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 0, is_ws=True)
        tracker.mark_notified(tracker.due(60_000), 60_000)
        restored = IncidentTracker(json.loads(json.dumps(tracker.snapshot())))
        restored.observe('WS:mark:CLOSED', None, 70_000)
        restored.observe('WS:mark:CLOSED', 'CLOSED', 70_001, is_ws=True)
        self.assertEqual(restored.due(700_001), {})

    def test_restart_offline_time_cannot_prove_thirty_minute_recovery(self):
        tracker = IncidentTracker()
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 0, is_ws=True)
        tracker.mark_notified(tracker.due(60_000), 60_000)
        tracker.observe('WS:mark:CLOSED', None, 60_001)
        restored = IncidentTracker(tracker.snapshot(), restart_ms=10_000_000)
        restored.observe('WS:mark:CLOSED', 'CLOSED', 10_000_001, is_ws=True)
        self.assertEqual(restored.due(20_000_000), {})

    def test_restart_unnotified_failure_requires_observed_grace(self):
        tracker = IncidentTracker()
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 0, is_ws=True)
        restored = IncidentTracker(tracker.snapshot(), restart_ms=10_000_000)
        self.assertEqual(restored.due(10_000_000), {})
        self.assertEqual(len(restored.due(10_060_000)), 1)

    def test_independent_causes_are_not_suppressed_by_other_incident(self):
        tracker = IncidentTracker()
        for cause in ('HTTP_403', 'HTTP_429', 'CLOSED', 'TIMEOUT', 'DATA'):
            tracker.observe('WS:mark:' + cause, cause, 0, is_ws=True)
        tracker.mark_notified(['WS:mark:CLOSED'], 60_000)
        self.assertEqual(set(tracker.due(60_000)), {
            'WS:mark:HTTP_403', 'WS:mark:HTTP_429', 'WS:mark:TIMEOUT', 'WS:mark:DATA'})

    def test_non_ws_serious_failure_is_immediately_due(self):
        tracker = IncidentTracker()
        tracker.observe('DATABASE', 'Database unavailable', 10)
        self.assertEqual(tracker.due(10), {'DATABASE': 'Database unavailable'})

    def test_failed_transaction_can_discard_clone(self):
        tracker = IncidentTracker()
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 0, is_ws=True)
        candidate = IncidentTracker(tracker.snapshot())
        candidate.mark_notified(candidate.due(60_000), 60_000)
        self.assertEqual(len(tracker.due(60_000)), 1)
        self.assertEqual(candidate.due(60_000), {})

    def test_snapshot_is_not_live_mutable_reference(self):
        tracker = IncidentTracker()
        tracker.observe('WS:mark:CLOSED', 'CLOSED', 0, is_ws=True)
        snapshot = tracker.snapshot()
        snapshot['WS:mark:CLOSED']['notified_ms'] = 1
        self.assertEqual(len(tracker.due(60_000)), 1)

    def test_invalid_old_records_ignored(self):
        self.assertEqual(IncidentTracker({'old': 'bad', 'also': {'first_ms': 'oops'}}).snapshot(), {})

    def test_safe_bounded_fault_codes(self):
        self.assertEqual(fault_code(ApiFault(403)), 'HTTP_403')
        self.assertEqual(fault_code(ApiFault(429)), 'HTTP_429')
        self.assertEqual(fault_code(ApiFault(0, 10, 'WebSocket disconnected')), 'CLOSED')
        self.assertEqual(fault_code(ApiFault(0, 10, 'Malformed WebSocket JSON')), 'DATA')
        self.assertEqual(fault_code(ApiFault(0, 10, 'No valid fresh stream rows')), 'DATA')
        self.assertEqual(fault_code(asyncio.TimeoutError('token-secret')), 'TIMEOUT')
        self.assertEqual(fault_code(ConnectionResetError('token-secret')), 'CLOSED')
        self.assertEqual(fault_code(ValueError('token-secret')), 'DATA')
        self.assertEqual(fault_code(aiohttp.ClientError('token-secret')), 'NETWORK')
        self.assertEqual(fault_code(RuntimeError('token-secret')), 'INTERNAL')


class Response:
    def __init__(self, status=200, value=None, headers=None, json_error=None):
        self.status, self.value = status, value
        self.headers = headers or {}
        self.json_error = json_error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def json(self, **kwargs):
        if self.json_error:
            raise self.json_error
        return self.value


class Session:
    def __init__(self, response=None, error=None):
        self.response, self.error = response, error

    def request(self, *args, **kwargs):
        if self.error:
            raise self.error
        return self.response


class ApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_passes_inline_buttons_and_returns_message(self):
        telegram = Telegram(None, 'fake-token', '123')
        telegram.call = AsyncMock(return_value={'message_id': 42})
        markup = {'inline_keyboard': [[{'text': 'Approve', 'callback_data': 'approve:XUSDT'}]]}
        self.assertEqual(await telegram.send('request', markup), {'message_id': 42})
        method, payload = telegram.call.await_args.args
        self.assertEqual(method, 'sendMessage')
        self.assertEqual(payload['reply_markup'], markup)
        self.assertEqual(payload['chat_id'], '123')

    async def test_signal_send_keeps_existing_payload(self):
        telegram = Telegram(None, 'fake-token', '123')
        telegram.call = AsyncMock()
        await telegram.send('A signal unchanged')
        self.assertEqual(telegram.call.await_args.args, ('sendMessage', {
            'chat_id': '123', 'text': 'A signal unchanged',
            'link_preview_options': {'is_disabled': True}}))

    async def test_updates_requests_messages_and_callbacks(self):
        telegram = Telegram(None, 'fake-token', '123')
        telegram.call = AsyncMock(return_value=[])
        await telegram.updates(25)
        self.assertEqual(telegram.call.await_args.args[1]['allowed_updates'], ['message', 'callback_query'])
        self.assertEqual(telegram.call.await_args.args[1]['offset'], 25)

    async def test_callback_answer_and_edit_transport(self):
        telegram = Telegram(None, 'fake-token', '123')
        telegram.call = AsyncMock(return_value=True)
        await telegram.answer_callback('id', 'Denied', alert=True)
        self.assertEqual(telegram.call.await_args.args, ('answerCallbackQuery', {
            'callback_query_id': 'id', 'text': 'Denied', 'show_alert': True}))
        await telegram.edit(42, 'Approved', {'inline_keyboard': []})
        self.assertEqual(telegram.call.await_args.args, ('editMessageText', {
            'chat_id': '123', 'message_id': 42, 'text': 'Approved',
            'link_preview_options': {'is_disabled': True}, 'reply_markup': {'inline_keyboard': []}}))

    async def test_callback_text_is_bounded(self):
        telegram = Telegram(None, 'fake-token', '123')
        telegram.call = AsyncMock(return_value=True)
        await telegram.answer_callback('id', 'x' * 500)
        self.assertEqual(len(telegram.call.await_args.args[1]['text']), 200)

    async def test_http_block_and_rate_limit_remain_distinct(self):
        for status, expected in ((403, 1800), (451, 1800), (429, 123), (418, 123)):
            with self.subTest(status=status):
                response = Response(status, {'parameters': {'retry_after': 123}}, {'Retry-After': '10'})
                with self.assertRaises(ApiFault) as caught:
                    await json_request(Session(response), 'GET', 'https://example.invalid/secret-token')
                self.assertEqual(caught.exception.delay, expected)
                self.assertEqual(fault_code(caught.exception), 'HTTP_' + str(status))
                self.assertNotIn('secret-token', str(caught.exception))

    async def test_timeout_close_network_and_json_data_are_distinct(self):
        cases = [(asyncio.TimeoutError('private'), 'TIMEOUT'),
                 (aiohttp.ServerDisconnectedError('private'), 'CLOSED'),
                 (ConnectionResetError('private'), 'CLOSED'),
                 (aiohttp.ClientError('private'), 'NETWORK')]
        for error, expected in cases:
            with self.subTest(expected=expected):
                with self.assertRaises(ApiFault) as caught:
                    await json_request(Session(error=error), 'GET', 'https://example.invalid/secret-token')
                self.assertEqual(fault_code(caught.exception), expected)
                self.assertNotIn('private', str(caught.exception))
        with self.assertRaises(ApiFault) as caught:
            await json_request(Session(Response(200, json_error=ValueError('private'))), 'GET', 'https://example.invalid')
        self.assertEqual(fault_code(caught.exception), 'DATA')

    async def test_binance_shared_cooldown_kept_for_http_rejection(self):
        binance = Binance(Session(Response(403)))
        binance.weights.take = AsyncMock()
        with self.assertRaises(ApiFault):
            await binance.get('/fundingInfo')
        self.assertGreater(binance.blocked_until, 0)

    async def test_existing_delayed_signal_wording_unchanged(self):
        body = 'original A/B text'
        row = {'body': body, 'detected_ms': 0, 'attempts': 0}
        self.assertEqual(delayed_message(row, 59_999), body)
        self.assertEqual(delayed_message(row, 60_000), body + '\n지연 발송: 장애 또는 전송 대기 후 재전송. 원래 감지시각은 위 메시지에 표시됩니다.')


class AppIncidentTests(unittest.IsolatedAsyncioTestCase):
    def bot(self):
        # No DB, HTTP session or real Telegram client is used by these checks.
        from app import Bot
        bot = Bot(None, None, 'fake-token', '123', seed={})
        bot.ready = True
        bot.clock_ms = 0
        bot.now = lambda: bot.clock_ms
        bot.save = AsyncMock()
        return bot

    async def test_application_ws_grace_flap_and_thirty_minute_rearm(self):
        bot = self.bot()
        await bot.service_problem('WS:mark', 'CLOSED')
        bot.clock_ms = 59_999
        await bot.diagnostics()
        self.assertEqual(bot.save.await_args.kwargs['events'], [])
        bot.clock_ms = 60_000
        await bot.diagnostics()
        self.assertEqual(len(bot.save.await_args.kwargs['events']), 1)
        self.assertIn('SERVICE:WS:mark:CLOSED', bot.save.await_args.kwargs['events'][0].message)
        bot.clock_ms = 61_000
        await bot.service_problem('WS:mark')
        bot.clock_ms = 62_000
        await bot.service_problem('WS:mark', 'CLOSED')
        bot.clock_ms = 122_000
        await bot.diagnostics()
        self.assertEqual(bot.save.await_args.kwargs['events'], [])
        bot.clock_ms = 130_000
        await bot.service_problem('WS:mark')
        bot.clock_ms += STABLE_RECOVERY_MS
        await bot.service_problem('WS:mark', 'CLOSED')
        bot.clock_ms += 60_000
        await bot.diagnostics()
        self.assertEqual(len(bot.save.await_args.kwargs['events']), 1)

    async def test_application_independent_fault_causes_notify_independently(self):
        bot = self.bot()
        await bot.service_problem('WS:mark', 'CLOSED')
        bot.clock_ms = 60_000
        await bot.diagnostics()
        bot.clock_ms = 70_000
        await bot.service_problem('WS:mark', 'HTTP_403')
        bot.clock_ms = 130_000
        await bot.diagnostics()
        events = bot.save.await_args.kwargs['events']
        self.assertEqual(len(events), 1)
        self.assertIn('HTTP_403', events[0].message)
        self.assertNotIn('CLOSED', events[0].message)

    async def test_application_restart_keeps_notified_service_suppressed(self):
        bot = self.bot()
        await bot.service_problem('WS:mark', 'TIMEOUT')
        bot.clock_ms = 60_000
        await bot.diagnostics()
        restored = self.bot()
        restored.services = dict(bot.services)
        restored.incidents = IncidentTracker(json.loads(json.dumps(bot.incidents.snapshot())), restart_ms=65_000)
        restored.clock_ms = 700_000
        await restored.diagnostics()
        self.assertEqual(restored.save.await_args.kwargs['events'], [])

    async def test_application_diagnostic_flag_and_outbox_share_save(self):
        bot = self.bot()
        await bot.service_problem('WS:mark', 'DATA')
        bot.clock_ms = 60_000
        await bot.diagnostics()
        payload = bot.save.await_args.kwargs
        self.assertEqual(len(payload['events']), 1)
        self.assertEqual(payload['events'][0].kind, 'DIAGNOSTIC')
        self.assertEqual(payload['meta']['incidents']['SERVICE:WS:mark:DATA']['notified_ms'], 60_000)

    async def test_application_failed_diagnostic_save_does_not_mark_notified(self):
        bot = self.bot()
        await bot.service_problem('WS:mark', 'CLOSED')
        bot.clock_ms = 60_000
        bot.save.side_effect = RuntimeError('Mock DB unavailable')
        with self.assertRaises(RuntimeError):
            await bot.diagnostics()
        self.assertEqual(bot.incidents.records['SERVICE:WS:mark:CLOSED']['notified_ms'], None)
        bot.save.side_effect = None
        await bot.diagnostics()
        self.assertEqual(len(bot.save.await_args.kwargs['events']), 1)

    async def test_application_db_failure_retains_actual_fault_visibility(self):
        bot = self.bot()
        bot.save.side_effect = RuntimeError('Mock DB unavailable')
        await bot.service_problem('WS:mark', 'TIMEOUT')
        self.assertEqual(bot.services, {'WS:mark': 'TIMEOUT'})
        self.assertEqual(bot.incidents.snapshot(), {})


if __name__ == '__main__':
    unittest.main()
