"""Focused offline regressions for listing age, diagnostic volume and DB writes."""
import copy
from decimal import Decimal as D
import unittest
from unittest.mock import AsyncMock, Mock

import app
from engine import (Candle, DAY_MS, FOUR_HOURS_MS, HOUR_MS, State, evaluate,
                    from_dict, minimum_close, problems, to_dict)
from storage import Store
from test_engine import NOW, fill_history, ready
from test_runtime import Connection, bot_fixture


def exchange_row(symbol='NEWUSDT', onboard=None, status='TRADING'):
    return {'symbol': symbol, 'baseAsset': symbol[:-4], 'quoteAsset': 'USDT',
            'marginAsset': 'USDT', 'contractType': 'PERPETUAL',
            'status': status, 'onboardDate': onboard if onboard is not None else NOW - 30 * DAY_MS}


def postgres_fixture():
    store = Store('postgresql://mock.invalid/offline', max_pending=2000)
    store.conn = Connection()
    bot = app.Bot(store, Mock(), 'fake-token', '100', 'fake-key')
    bot.ready = True
    bot.now = lambda: NOW
    bot.started_ms = NOW - 120_000
    return bot, store


async def settled_states(bot, store, count=300, interval=4, floor=D('-0.02')):
    """Persist genuine baseline event state, then reset observed SQL counters."""
    bot.states.clear()
    rows = []
    for index in range(count):
        state = ready(interval)
        state.symbol = f'COIN{index:03}USDT'
        state.base = f'COIN{index:03}'
        state.price = D('2')
        state.funding = D('-0.025')
        state.next_times = [state.next_funding_ms]
        rows.append({'symbol': state.symbol, 'fundingIntervalHours': interval,
                     'adjustedFundingRateFloor': str(floor)})
        bot.states[state.symbol] = state
    bot.adjustments = {row['symbol']: row for row in rows}
    bot.adjustments_ms = NOW
    for state in bot.states.values():
        bot.apply_rule(state)
        self_events = evaluate(state, NOW)
        if self_events:
            raise AssertionError('Baseline must not generate signal messages')
    await bot.save(list(bot.states.values()), candles=True)
    store.conn.executions.clear()
    store.conn.commits = 0
    bot.binance.get = AsyncMock(return_value=rows)
    return rows


class ListingRevisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_newly_discovered_30_day_listing_uses_real_age_immediately(self):
        bot, store, _, _ = bot_fixture()
        bot.states.clear()
        bot.meta['initialized'] = True
        listed = NOW - 30 * DAY_MS
        row = exchange_row(onboard=listed)
        bot.binance.get = AsyncMock(return_value={'symbols': [row]})
        await bot.discover()
        discovered = bot.states[row['symbol']]
        self.assertEqual(discovered.onboard_ms, listed)
        self.assertEqual(discovered.source_onboard_ms, listed)
        self.assertEqual(store.snapshots[row['symbol']]['onboard_ms'], listed)
        eligible = ready(now=NOW, onboard=discovered.onboard_ms)
        eligible.symbol, eligible.base = discovered.symbol, discovered.base
        eligible.source_onboard_ms = discovered.source_onboard_ms
        bot.states[eligible.symbol] = eligible
        await bot.assess([eligible.symbol])
        self.assertEqual(len(store.pending), 1)
        self.assertTrue(bot.states[eligible.symbol].a_sent)

    async def test_newly_discovered_two_day_listing_waits_only_remaining_120_hours(self):
        bot, store, _, _ = bot_fixture()
        bot.states.clear()
        bot.meta['initialized'] = True
        listed = NOW - 48 * HOUR_MS
        row = exchange_row(onboard=listed)
        bot.binance.get = AsyncMock(return_value={'symbols': [row]})
        await bot.discover()
        self.assertEqual(bot.states[row['symbol']].onboard_ms, listed)
        before = listed + 168 * HOUR_MS - 1
        candidate = ready(now=before, onboard=listed)
        candidate.symbol, candidate.base = row['symbol'], row['baseAsset']
        bot.states[candidate.symbol] = candidate
        bot.now = lambda: before
        await bot.assess([candidate.symbol])
        self.assertEqual(store.pending, {})
        exact = before + 1
        state = bot.states[candidate.symbol]
        fill_history(state, exact)
        state.price_ms = state.funding_ms = state.interval_ms = exact
        bot.now = lambda: exact
        await bot.assess([candidate.symbol])
        self.assertEqual(len(store.pending), 1)

    async def test_first_seven_days_price_low_remains_in_30_day_history(self):
        bot, store, _, _ = bot_fixture()
        bot.states.clear()
        bot.meta['initialized'] = True
        listed = NOW - 30 * DAY_MS
        row = exchange_row(onboard=listed)
        bot.binance.get = AsyncMock(return_value={'symbols': [row]})
        await bot.discover()
        discovered = bot.states[row['symbol']]
        state = ready(now=NOW, onboard=discovered.onboard_ms)
        state.symbol, state.base = discovered.symbol, discovered.base
        state.source_onboard_ms = discovered.source_onboard_ms
        state.history_ready, state.candles = False, []
        state.price = D('1.5')
        bot.states[state.symbol] = state
        start = listed // FOUR_HOURS_MS * FOUR_HOURS_MS
        end = NOW // FOUR_HOURS_MS * FOUR_HOURS_MS
        page = [[opened, '0', '0', '0', '0.5' if opened < listed + 7 * DAY_MS else '1.5',
                 '0', opened + FOUR_HOURS_MS - 1]
                for opened in range(start, end, FOUR_HOURS_MS)]
        bot.binance.get = AsyncMock(return_value=page)
        await bot.fetch_candles(state)
        args = bot.binance.get.await_args
        self.assertEqual(args.args[0], '/klines')
        self.assertEqual(args.args[1]['startTime'], start)
        self.assertEqual(minimum_close(bot.states[state.symbol], NOW), D('0.5'))
        self.assertEqual(len(store.pending), 1)
        self.assertTrue(any(c.close_ms < listed + 7 * DAY_MS for c in bot.states[state.symbol].candles))

    async def test_rediscovery_preserves_existing_a_epoch_and_pending_event(self):
        bot, store, _, state = bot_fixture()
        await bot.assess([state.symbol])
        epoch = bot.states[state.symbol].a_epoch
        row = exchange_row(state.symbol, state.onboard_ms)
        bot.meta['initialized'] = True
        bot.rows = {state.symbol: row}
        bot.binance.get = AsyncMock(return_value={'symbols': [row]})
        await bot.discover()
        self.assertEqual(bot.states[state.symbol].a_epoch, epoch)
        self.assertTrue(bot.states[state.symbol].a_sent)
        self.assertEqual(len(store.pending), 1)

    async def test_confirmed_relisting_without_new_time_remains_unknown_across_restart(self):
        bot, store, _, old = bot_fixture()
        row = exchange_row(old.symbol, old.onboard_ms)
        bot.meta['initialized'] = True
        bot.rows = {old.symbol: row}
        bot.binance.get = AsyncMock(return_value={'symbols': [{**row, 'status': 'CLOSE'}]})
        await bot.discover()
        bot.now = lambda: NOW + DAY_MS
        bot.binance.get.return_value = {'symbols': [row]}
        await bot.discover()
        relisted = bot.states[old.symbol]
        self.assertEqual(relisted.onboard_ms, 0)
        self.assertEqual(relisted.listing_status, 'RELISTING_TIME_UNKNOWN')
        self.assertEqual(relisted.source_onboard_ms, old.onboard_ms)
        restored = from_dict(copy.deepcopy(store.snapshots[old.symbol]))
        self.assertEqual(restored.listing_status, 'RELISTING_TIME_UNKNOWN')
        self.assertEqual(restored.onboard_ms, 0)
        eligible = ready(now=NOW + DAY_MS)
        eligible.symbol = relisted.symbol
        eligible.onboard_ms = restored.onboard_ms
        eligible.listing_status = restored.listing_status
        self.assertEqual(evaluate(eligible, NOW + DAY_MS), [])
        self.assertIn('LISTING_UNKNOWN', problems(eligible, NOW + DAY_MS))
        bot.states[old.symbol] = restored
        await bot.discover()
        self.assertEqual(bot.states[old.symbol].onboard_ms, 0)
        self.assertEqual(store.pending, {})

    async def test_confirmed_relisting_uses_verified_changed_upstream_time(self):
        bot, _, _, old = bot_fixture()
        row = exchange_row(old.symbol, old.onboard_ms)
        bot.meta['initialized'] = True
        bot.rows = {old.symbol: row}
        bot.binance.get = AsyncMock(return_value={'symbols': [{**row, 'status': 'CLOSE'}]})
        await bot.discover()
        relisted_at = NOW + HOUR_MS
        bot.now = lambda: relisted_at + HOUR_MS
        updated = {**row, 'onboardDate': relisted_at}
        bot.binance.get.return_value = {'symbols': [updated]}
        await bot.discover()
        self.assertEqual(bot.states[old.symbol].onboard_ms, relisted_at)
        self.assertEqual(bot.states[old.symbol].listing_status, 'CONFIRMED')
        self.assertEqual(bot.states[old.symbol].source_onboard_ms, relisted_at)

    async def test_invalid_future_listing_does_not_invent_observed_timestamp(self):
        for upstream in (0, NOW + DAY_MS, True, 'invalid'):
            with self.subTest(upstream=upstream):
                bot, _, _, _ = bot_fixture()
                bot.states.clear()
                row = exchange_row(onboard=upstream)
                bot.binance.get = AsyncMock(return_value={'symbols': [row]})
                await bot.discover()
                self.assertEqual(bot.states[row['symbol']].onboard_ms, 0)
                self.assertNotEqual(bot.states[row['symbol']].listing_status, 'CONFIRMED')


    async def test_existing_fabricated_age_correction_refetches_history_and_preserves_a_epoch(self):
        bot, store, _, state = bot_fixture()
        actual_listing = NOW - 30 * DAY_MS
        state.onboard_ms = NOW - 10 * DAY_MS
        state.source_onboard_ms = actual_listing
        fill_history(state, NOW)
        await bot.assess([state.symbol])
        existing = bot.states[state.symbol]
        self.assertEqual(minimum_close(existing, NOW), D('1'))
        old_epoch, old_revision = existing.a_epoch, existing.history_revision
        self.assertEqual(len(store.pending), 1)
        row = exchange_row(state.symbol, actual_listing)
        bot.meta['initialized'] = True
        bot.rows = {state.symbol: row}
        bot.binance.get = AsyncMock(return_value={'symbols': [row]})
        await bot.discover()
        corrected = bot.states[state.symbol]
        self.assertEqual(corrected.onboard_ms, actual_listing)
        self.assertEqual(corrected.a_epoch, old_epoch)
        self.assertTrue(corrected.a_sent)
        self.assertFalse(corrected.history_ready)
        self.assertGreater(corrected.history_revision, old_revision)
        self.assertEqual(corrected.history_cache_revision, -1)
        self.assertIsNone(minimum_close(corrected, NOW))
        self.assertEqual(len(store.pending), 1)
        start = actual_listing // FOUR_HOURS_MS * FOUR_HOURS_MS
        end = NOW // FOUR_HOURS_MS * FOUR_HOURS_MS
        page = [[opened, '0', '0', '0', '0.5' if opened < actual_listing + 7 * DAY_MS else '1',
                 '0', opened + FOUR_HOURS_MS - 1]
                for opened in range(start, end, FOUR_HOURS_MS)]
        bot.binance.get = AsyncMock(return_value=page)
        await bot.fetch_candles(corrected)
        self.assertEqual(bot.binance.get.await_args.args[1]['startTime'], start)
        restored = bot.states[state.symbol]
        self.assertTrue(restored.history_ready)
        self.assertEqual(minimum_close(restored, NOW), D('0.5'))
        self.assertTrue(any(c.close_ms < actual_listing + 7 * DAY_MS for c in restored.candles))
        self.assertEqual(restored.a_epoch, old_epoch)
        self.assertTrue(restored.a_sent)
        self.assertEqual(len(store.pending), 1)

class DiagnosticRevisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_300_unknown_symbols_produce_one_bounded_summary_and_no_repeat(self):
        bot, store, _, _ = bot_fixture()
        bot.states = {f'UNKNOWN{index:03}USDT': State(f'UNKNOWN{index:03}USDT',
            f'UNKNOWN{index:03}', NOW - 30 * DAY_MS) for index in range(300)}
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)
        body = next(iter(store.pending.values()))['body']
        self.assertLessEqual(len(body), 4096)
        self.assertIn('300', body)
        self.assertIn('COINGECKO_MATCH_UNKNOWN', body)
        self.assertIn('FDV_UNKNOWN', body)
        self.assertNotIn('UNKNOWN000USDT', body)
        self.assertNotIn('UNKNOWN299USDT', body)
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)

    async def test_same_diagnostic_type_membership_changes_do_not_resend(self):
        bot, store, _, first = bot_fixture()
        first.interval_hours = None
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)
        another = ready()
        another.symbol = 'SECONDUSDT'
        another.interval_hours = None
        bot.states[another.symbol] = another
        first.interval_hours = 1
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)
        first.interval_hours = None
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)

    async def test_important_service_fault_is_immediate_and_recurrence_is_new_event(self):
        bot, store, _, _ = bot_fixture()
        bot.started_ms = NOW
        await bot.service_problem('EXCHANGE', 'ApiFault HTTP 503')
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)
        self.assertIn('EXCHANGE', next(iter(store.pending.values()))['body'])
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)
        await bot.service_problem('EXCHANGE')
        await bot.diagnostics()
        await bot.service_problem('EXCHANGE', 'ApiFault HTTP 503')
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 2)

    async def test_many_candle_service_faults_share_one_type_then_new_fault_is_not_hidden(self):
        bot, store, _, state = bot_fixture()
        bot.services = {f'KLINES:FAULT{index:03}USDT': 'ApiFault HTTP 503' for index in range(300)}
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)
        first = next(iter(store.pending.values()))['body']
        self.assertLessEqual(len(first), 4096)
        self.assertIn('300', first)
        bot.services['COINGECKO'] = 'ApiFault HTTP 429'
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 2)
        self.assertTrue(any('COINGECKO' in row['body'] for row in store.pending.values()))


    async def test_initial_progressing_metadata_collection_is_quiet_and_visible_in_status(self):
        bot, store, _, _ = bot_fixture()
        bot.states.clear()
        for index in range(300):
            state = ready()
            state.symbol = f'PENDING{index:03}USDT'
            state.base = f'PENDING{index:03}'
            state.classification = 'UNKNOWN'
            state.mapping_valid, state.cg_id = False, None
            state.market_cap = state.fdv = None
            bot.states[state.symbol] = state
        bot.cg_refresh_active = True
        bot.cg_progress_ms = NOW
        bot.cg_initial_done = False
        await bot.diagnostics()
        self.assertEqual(store.pending, {})
        bot.states = {f'PENDING{index:03}USDT': State(f'PENDING{index:03}USDT',
            f'PENDING{index:03}', NOW - 30 * DAY_MS) for index in range(300)}
        await bot.diagnostics()
        self.assertEqual(store.pending, {})
        summary = await bot.status()
        self.assertTrue(any('매칭' in line and '300' in line for line in summary.splitlines()), summary)
        self.assertTrue(any('FDV' in line and '미확인' in line and '300' in line
                            for line in summary.splitlines()), summary)
        detail = await bot.status('PENDING000USDT')
        self.assertIn('COINGECKO_MATCH_UNKNOWN', detail)
        self.assertIn('FDV_UNKNOWN', detail)
        await bot.service_problem('COINGECKO', 'ApiFault HTTP 429')
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)
        self.assertIn('COINGECKO', next(iter(store.pending.values()))['body'])
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)

    async def test_status_problem_pages_preserve_symbol_details(self):
        bot, _, _, _ = bot_fixture()
        bot.states.clear()
        symbols = []
        for index in range(31):
            state = ready()
            state.symbol = f'PROBLEM{index:03}USDT'
            state.base = f'PROBLEM{index:03}'
            state.interval_hours = None
            bot.states[state.symbol] = state
            symbols.append(state.symbol)
        first_page = await bot.status('1')
        second_page = await bot.status('2')
        third_page = await bot.status('3')
        first_symbols = {symbol for symbol in symbols if symbol in first_page}
        second_symbols = {symbol for symbol in symbols if symbol in second_page}
        third_symbols = {symbol for symbol in symbols if symbol in third_page}
        self.assertEqual(len(first_symbols), 15)
        self.assertEqual(len(second_symbols), 15)
        self.assertEqual(len(third_symbols), 1)
        self.assertFalse(first_symbols & second_symbols)
        self.assertFalse(second_symbols & third_symbols)
        self.assertEqual(first_symbols | second_symbols | third_symbols, set(symbols))
        self.assertIn('INTERVAL_UNKNOWN', await bot.status(symbols[-1]))

    async def test_failed_initial_collection_keeps_active_diagnostics_through_progressing_retries(self):
        bot, store, _, state = bot_fixture()
        state.classification = 'UNKNOWN'
        state.mapping_valid, state.cg_id = False, None
        state.market_cap = state.fdv = None
        await bot.service_problem('COINGECKO', 'ApiFault HTTP 429')
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)
        active = copy.deepcopy(bot.meta['diagnostics'])
        self.assertIn(state.symbol + ':CLASSIFICATION_UNKNOWN', active)
        self.assertIn(state.symbol + ':COINGECKO_MATCH_UNKNOWN', active)
        self.assertIn(state.symbol + ':FDV_UNKNOWN', active)
        for offset in (1_000, 2_000, 3_000):
            bot.now = lambda value=NOW + offset: value
            bot.cg_refresh_active = True
            bot.cg_progress_ms = bot.now()
            await bot.diagnostics()
            self.assertEqual(bot.meta['diagnostics'], active)
            bot.cg_refresh_active = False
            await bot.service_problem('COINGECKO', 'ApiFault HTTP 429')
            await bot.diagnostics()
            self.assertEqual(len(store.pending), 1)
        state.classification = 'CRYPTO'
        state.mapping_valid, state.cg_id = True, 'independent-alt'
        state.market_cap, state.fdv = D('9499'), D('10000')
        await bot.diagnostics()
        self.assertNotIn(state.symbol + ':COINGECKO_MATCH_UNKNOWN', bot.meta['diagnostics'])
        state.classification = 'UNKNOWN'
        state.mapping_valid, state.cg_id = False, None
        state.market_cap = state.fdv = None
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 2)

    async def test_new_price_websocket_fault_is_separate_from_ongoing_mark_fault(self):
        bot, store, _, _ = bot_fixture()
        await bot.service_problem('WS:mark', 'ApiFault HTTP 503')
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 1)
        await bot.service_problem('WS:price', 'ApiFault HTTP 503')
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 2)
        messages = [row['body'] for row in store.pending.values()]
        self.assertTrue(any('WS:mark' in body for body in messages))
        self.assertTrue(any('WS:price' in body for body in messages))
        await bot.diagnostics()
        self.assertEqual(len(store.pending), 2)

class PersistenceRevisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_stable_one_minute_refresh_for_300_symbols_writes_no_symbol_rows(self):
        bot, store = postgres_fixture()
        await settled_states(bot, store)
        refresh_ms = NOW + 60_000
        bot.now = lambda: refresh_ms
        await bot.refresh_funding()
        self.assertEqual(sum(q.startswith('INSERT INTO bot_symbols') for q, _, _ in store.conn.executions), 0)
        self.assertEqual(sum(q.startswith('INSERT INTO bot_candles') for q, _, _ in store.conn.executions), 0)
        self.assertEqual(store.conn.pending, {})
        self.assertTrue(all(s.interval_ms == refresh_ms and s.last_official_floor_ms == refresh_ms
                            for s in bot.states.values()))
        bot.now = lambda: refresh_ms + 60_000
        await bot.refresh_funding()
        self.assertEqual(sum(q.startswith('INSERT INTO bot_symbols') for q, _, _ in store.conn.executions), 0)

    async def test_one_floor_change_persists_one_symbol_and_b_outbox_atomically(self):
        bot, store = postgres_fixture()
        rows = await settled_states(bot, store, floor=D('-0.03'))
        target = bot.states[rows[0]['symbol']]
        target.price = D('3')
        refresh_ms = NOW + 60_000
        for state in bot.states.values():
            state.funding_ms = state.price_ms = refresh_ms
        updated = copy.deepcopy(rows)
        updated[0]['adjustedFundingRateFloor'] = '-0.02'
        bot.binance.get = AsyncMock(return_value=updated)
        bot.now = lambda: refresh_ms
        await bot.refresh_funding()
        writes = [(q, values, transactional) for q, values, transactional in store.conn.executions
                  if q.startswith('INSERT INTO bot_symbols')]
        self.assertEqual({values[0] for _, values, _ in writes}, {target.symbol})
        self.assertEqual(len(store.conn.pending), 1)
        event = next(iter(store.conn.pending.values()))
        self.assertEqual(event['symbol'], target.symbol)
        self.assertIn('[B]', event['body'])
        self.assertTrue(store.conn.states[target.symbol]['b_sent'])
        self.assertEqual(store.conn.states[target.symbol]['floor'], '-0.02')
        self.assertTrue(all(transactional for _, _, transactional in store.conn.executions))
        self.assertEqual(store.conn.commits, 1)
        restored = from_dict(store.conn.states[target.symbol])
        restored.candles = [Candle(item['open_ms'], item['close_ms'], D(item['close']))
                            for item in store.conn.candles[target.symbol]]
        self.assertEqual(evaluate(restored, refresh_ms), [])

    async def test_market_price_and_funding_samples_remain_memory_updates(self):
        bot, store = postgres_fixture()
        rows = await settled_states(bot, store, count=1)
        symbol = rows[0]['symbol']
        state = bot.states[symbol]
        for offset in (1_000, 2_000, 3_000):
            stamp = NOW + offset
            bot.now = lambda value=stamp: value
            await bot.ingest('price', [{'s': symbol, 'E': stamp, 'c': '2.1'}])
            await bot.ingest('mark', [{'s': symbol, 'E': stamp, 'r': '-0.024', 'T': state.next_funding_ms}])
        self.assertEqual(bot.states[symbol].price, D('2.1'))
        self.assertEqual(bot.states[symbol].funding, D('-0.024'))
        self.assertEqual(bot.states[symbol].funding_ms, NOW + 3_000)
        self.assertEqual(sum(q.startswith('INSERT INTO bot_symbols') for q, _, _ in store.conn.executions), 0)
        self.assertEqual(store.conn.pending, {})


    async def test_checkpoint_restore_only_promotes_official_sources_and_never_current_time(self):
        bot, store, _, _ = bot_fixture()
        old_ms = NOW - 3 * DAY_MS
        official = ready(4)
        official.symbol = 'OFFICIALUSDT'
        official.interval_source = official.floor_source = 'OFFICIAL_FUNDING_INFO'
        official.interval_ms = official.floor_ms = old_ms
        official.last_official_floor = official.floor
        official.last_official_floor_ms = old_ms
        fallback = copy.deepcopy(official)
        fallback.symbol = 'FALLBACKUSDT'
        fallback.floor_source = 'LAST_VERIFIED_OFFICIAL'
        estimate = copy.deepcopy(official)
        estimate.symbol = 'ESTIMATEUSDT'
        estimate.interval_source = 'OBSERVED_SETTLEMENTS'
        estimate.floor_source = 'OBSERVED_30D_MIN'
        estimate.floor_estimated = True
        snapshots = {state.symbol: to_dict(state) for state in (official, fallback, estimate)}
        checkpoint = NOW - 1_000
        store.close, store.open = AsyncMock(), AsyncMock()
        store.load = AsyncMock(return_value=(copy.deepcopy(snapshots), {'funding_checkpoint_ms': checkpoint}))
        await bot.connect_db()
        restored_official = bot.states[official.symbol]
        restored_fallback = bot.states[fallback.symbol]
        restored_estimate = bot.states[estimate.symbol]
        self.assertEqual(restored_official.interval_ms, checkpoint)
        self.assertEqual(restored_official.floor_ms, checkpoint)
        self.assertEqual(restored_official.last_official_floor_ms, checkpoint)
        self.assertEqual(restored_fallback.interval_ms, checkpoint)
        self.assertEqual(restored_fallback.floor_ms, old_ms)
        self.assertEqual(restored_fallback.last_official_floor_ms, old_ms)
        self.assertEqual(restored_estimate.interval_ms, old_ms)
        self.assertEqual(restored_estimate.floor_ms, old_ms)
        self.assertEqual(restored_estimate.last_official_floor_ms, old_ms)
        self.assertIn('FLOOR_UNKNOWN', problems(restored_fallback, NOW))
        self.assertTrue(all(state.price is None and state.funding is None
                            for state in bot.states.values()))
        stale_checkpoint = NOW - 300_001
        store.load = AsyncMock(return_value=(copy.deepcopy(snapshots),
                                             {'funding_checkpoint_ms': stale_checkpoint}))
        await bot.connect_db()
        stale = bot.states[official.symbol]
        self.assertEqual(stale.interval_ms, stale_checkpoint)
        self.assertEqual(stale.floor_ms, stale_checkpoint)
        self.assertIn('INTERVAL_UNKNOWN', problems(stale, NOW))
        stale.price, stale.price_ms = D('3'), NOW
        stale.funding, stale.funding_ms = D('-0.02'), NOW
        self.assertEqual(evaluate(stale, NOW), [])

    async def test_failed_save_does_not_mark_dirty_state_clean_before_successful_retry(self):
        bot, store = postgres_fixture()
        rows = await settled_states(bot, store, count=2)
        symbol = rows[0]['symbol']
        unchanged_symbol = rows[1]['symbol']
        baseline_signature = bot.saved_signatures[symbol]
        baseline_other = copy.deepcopy(store.conn.states[unchanged_symbol])
        new = copy.copy(bot.states[symbol])
        new.floor = new.last_official_floor = D('-0.021')
        store.conn.fail_on = 'INSERT INTO bot_symbols'
        with self.assertRaises(OSError):
            await bot.save([new])
        self.assertFalse(bot.ready)
        self.assertEqual(bot.saved_signatures[symbol], baseline_signature)
        self.assertEqual(store.conn.states[symbol]['floor'], '-0.02')
        self.assertEqual(store.conn.states[unchanged_symbol], baseline_other)
        store.conn.fail_on = None
        bot.ready = True
        await bot.save([new])
        self.assertEqual(bot.saved_signatures[symbol], app.durable_signature(new))
        self.assertNotEqual(bot.saved_signatures[symbol], baseline_signature)
        self.assertEqual(store.conn.states[symbol]['floor'], '-0.021')
        self.assertEqual(sum(q.startswith('INSERT INTO bot_symbols') for q, _, _ in store.conn.executions), 1)
        self.assertEqual(store.conn.commits, 1)
        self.assertEqual(store.conn.rollbacks, 1)
        self.assertEqual(store.conn.states[unchanged_symbol], baseline_other)

class StatusCoverageTests(unittest.IsolatedAsyncioTestCase):
    async def test_summary_counts_overlap_and_monitorability_does_not_require_a_signal(self):
        bot, _, _, _ = bot_fixture()
        symbols = ['BTCUSDT', 'GOLDUSDT', 'AMBIGUSDT', 'FDVMISSINGUSDT',
                   'FDV95USDT', 'YOUNGUSDT', 'READYUSDT', 'HISTORYUSDT', 'UNKNOWNUSDT']
        bot.rows = {symbol: exchange_row(symbol) for symbol in symbols}
        bot.states.clear()
        for symbol in symbols[1:]:
            state = ready()
            state.symbol, state.base = symbol, symbol[:-4]
            bot.states[symbol] = state
        bot.states['GOLDUSDT'].classification = 'EXCLUDED'
        ambiguous = bot.states['AMBIGUSDT']
        ambiguous.classification = 'UNKNOWN'
        ambiguous.mapping_valid, ambiguous.cg_id = False, None
        ambiguous.market_cap = ambiguous.fdv = None
        bot.states['FDVMISSINGUSDT'].fdv = None
        bot.states['FDV95USDT'].market_cap = D('9500')
        young = bot.states['YOUNGUSDT']
        young.onboard_ms = NOW - 6 * DAY_MS
        fill_history(young, NOW)
        monitorable = bot.states['READYUSDT']
        monitorable.price, monitorable.funding = D('2'), D('0.001')
        bot.states['HISTORYUSDT'].history_ready = False
        bot.states['HISTORYUSDT'].candles = []
        bot.states['UNKNOWNUSDT'].classification = 'UNKNOWN'
        summary = await bot.status()
        expected = [(('전체', 'USDT-M', '무기한'), 9), (('제외', '지정'), 1),
                    (('자산연계', '제외'), 1), (('CoinGecko', '매칭'), 1),
                    (('FDV', '미확인'), 2), (('FDV', '95'), 1),
                    (('상장', '대기'), 1), (('실제', '감시', '가능'), 1)]
        for words, count in expected:
            with self.subTest(category=words):
                lines = [line for line in summary.splitlines()
                         if all(word in line for word in words)]
                self.assertTrue(lines, summary)
                self.assertRegex(lines[0], rf'[:：]\s*{count}(?:\s*개)?(?:\s|$)')
        self.assertIn('중복', summary)
        self.assertIn('COINGECKO_MATCH_UNKNOWN', await bot.status('AMBIGUSDT'))
        self.assertIn('FDV_UNKNOWN', await bot.status('AMBIGUSDT'))
        self.assertIn('CLASSIFICATION_UNKNOWN', await bot.status('UNKNOWNUSDT'))

if __name__ == '__main__':
    unittest.main()






