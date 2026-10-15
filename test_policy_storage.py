"""Offline SQL and transaction fakes: never connects to the operating database."""
import copy
import json
import unittest
from decimal import Decimal

from engine import Candle, Event, State, from_dict, to_dict
from policy import SEED_META_KEY, load_seed, validate_seed
from storage import Backpressure, PolicyConflict, SCHEMA, Store


class Transaction:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.previous = copy.deepcopy((self.conn.watch, self.conn.states, self.conn.candles,
                                       self.conn.meta, self.conn.pending))
        self.conn.depth += 1
        return self

    async def __aexit__(self, kind, value, trace):
        self.conn.depth -= 1
        if kind:
            (self.conn.watch, self.conn.states, self.conn.candles,
             self.conn.meta, self.conn.pending) = self.previous


class FakeConnection:
    def __init__(self):
        self.watch, self.states, self.candles, self.meta, self.pending = {}, {}, {}, {}, {}
        self.statements = []
        self.depth = 0
        self.fail = ''
        self.cursor_symbols = None

    def transaction(self):
        return Transaction(self)

    async def fetchval(self, sql, *args):
        self.statements.append((sql, args))
        if sql.startswith('SELECT value FROM bot_meta'):
            return self.meta.get(args[0])
        if sql == 'SELECT count(*) FROM bot_watch':
            return len(self.watch)
        if sql == 'SELECT count(*) FROM bot_pending':
            return len(self.pending)
        if sql.startswith('SELECT candles FROM bot_candles'):
            return self.candles.get(args[0])
        raise AssertionError(sql)

    async def fetch(self, sql, *args):
        self.statements.append((sql, args))
        if sql.startswith('SELECT symbol,status,generation FROM bot_watch'):
            return [dict(row) for row in self.watch.values()]
        if sql == 'SELECT * FROM bot_watch':
            return [dict(row) for row in self.watch.values()]
        if sql == 'SELECT key,value FROM bot_meta':
            return [{'key': key, 'value': value} for key, value in self.meta.items()]
        raise AssertionError(sql)

    async def fetchrow(self, sql, *args):
        self.statements.append((sql, args))
        if sql.startswith('SELECT generation,status'):
            row = self.watch.get(args[0])
            return dict(row) if row else None
        if sql.startswith('SELECT * FROM bot_pending'):
            rows = [row for row in self.pending.values() if row['due_ms'] <= args[0]]
            return dict(min(rows, key=lambda row: (row['detected_ms'], row['id']))) if rows else None
        raise AssertionError(sql)

    async def execute(self, sql, *args):
        self.statements.append((sql, args))
        if self.fail and self.fail in sql:
            raise RuntimeError('injected SQL failure')
        if sql.startswith('INSERT INTO bot_watch'):
            if len(args) == 3:
                symbol, status, updated_ms = args
                self.watch[symbol] = dict(symbol=symbol, status=status, generation=1,
                    listing_key='', notice_id='', message_id=None, updated_ms=updated_ms)
            else:
                symbol, status, generation, listing_key, notice_id, message_id, updated_ms = args
                if symbol in self.watch:
                    raise RuntimeError('duplicate primary key')
                self.watch[symbol] = dict(symbol=symbol, status=status, generation=generation,
                    listing_key=listing_key, notice_id=notice_id, message_id=message_id, updated_ms=updated_ms)
        elif sql.startswith('UPDATE bot_watch SET status='):
            symbol, status, generation, listing_key, notice_id, message_id, updated_ms = args
            self.watch[symbol].update(status=status, generation=generation, listing_key=listing_key,
                notice_id=notice_id, message_id=message_id, updated_ms=updated_ms)
        elif sql.startswith('UPDATE bot_watch SET message_id='):
            for row in self.watch.values():
                if row['notice_id'] == args[0]:
                    row['message_id'] = args[1]
        elif sql.startswith('INSERT INTO bot_meta'):
            self.meta[args[0]] = args[1]
        elif sql.startswith('INSERT INTO bot_symbols'):
            self.states[args[0]] = args[1]
        elif sql.startswith('INSERT INTO bot_candles'):
            self.candles[args[0]] = args[1]
        elif sql.startswith('INSERT INTO bot_pending'):
            key, symbol, body, detected_ms, markup = args
            self.pending.setdefault(key, dict(id=key, symbol=symbol, body=body, detected_ms=detected_ms,
                reply_markup=markup, attempts=0, due_ms=0))
        elif sql.startswith('DELETE FROM bot_pending'):
            self.pending.pop(args[0], None)
        elif sql.startswith('UPDATE bot_pending SET attempts='):
            self.pending[args[0]].update(attempts=args[1], due_ms=args[2])
        else:
            raise AssertionError(sql)
        return 'OK'

    async def executemany(self, sql, rows):
        for row in rows:
            await self.execute(sql, *row)

    def cursor(self, sql, approved, prefetch):
        assert self.depth > 0
        assert 'AND s.symbol = ANY($1::text[])' in sql
        assert prefetch <= 16
        self.cursor_symbols = set(approved)
        async def rows():
            for symbol, state in self.states.items():
                yield dict(symbol=symbol, state=state,
                    candles=self.candles.get(symbol) if symbol in self.cursor_symbols else None)
        return rows()


class SeedValidationTests(unittest.TestCase):
    def test_confirmed_seed_partition(self):
        seed = load_seed()
        self.assertEqual([len(seed[k]) for k in ('snapshot','approved','blocked','review')], [525,337,187,1])
        self.assertEqual(seed['review'], ['1000RATSUSDT'])
        self.assertIn('哈基米USDT', seed['snapshot'])
        self.assertEqual(len(set(seed['snapshot'])), 525)

    def test_duplicate_or_changed_seed_fails_closed(self):
        seed = load_seed()
        seed['approved'][0] = seed['approved'][1]
        with self.assertRaises(ValueError):
            validate_seed(seed)
        seed = load_seed()
        seed['approved'][0], seed['blocked'][0] = seed['blocked'][0], seed['approved'][0]
        with self.assertRaises(ValueError):
            validate_seed(seed)

    def test_schema_is_only_additive(self):
        self.assertNotIn('DROP ', SCHEMA.upper())
        self.assertNotIn('TRUNCATE ', SCHEMA.upper())
        self.assertIn('ADD COLUMN IF NOT EXISTS reply_markup JSONB', SCHEMA)

    def test_slots_keep_serialization_and_signal_flags(self):
        state = State('EXUSDT','EX',1, a_epoch='existing', a_sent=True, b_epoch='other', b_sent=True)
        state.candles = [Candle(0,14399999,Decimal('2'))]
        self.assertFalse(hasattr(state,'__dict__'))
        self.assertFalse(hasattr(state.candles[0],'__dict__'))
        restored = from_dict(to_dict(state))
        self.assertEqual(restored.a_epoch, state.a_epoch)
        self.assertTrue(restored.a_sent and restored.b_sent)
        self.assertEqual(restored.candles, state.candles)


class PolicyStorageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.conn = FakeConnection()
        self.store = Store('offline-only')
        self.store.conn = self.conn

    async def test_initial_seed_is_exact_and_restart_preserves_manual_choice(self):
        self.assertTrue(await self.store.seed_policy(load_seed()))
        policy = await self.store.load_policy()
        self.assertEqual(len(policy), 525)
        self.assertEqual({status:sum(r['status']==status for r in policy.values())
                          for status in ('APPROVED','BLOCKED','REVIEW')},
                         {'APPROVED':337,'BLOCKED':187,'REVIEW':1})
        self.assertFalse(self.conn.pending)
        symbol = load_seed()['approved'][0]
        row = dict(policy[symbol],status='BLOCKED', generation=2, expected_generation=1)
        await self.store.commit(watch=[row])
        self.assertFalse(await self.store.seed_policy(load_seed()))
        self.assertEqual((await self.store.load_policy())[symbol]['status'],'BLOCKED')
        self.assertEqual(self.conn.watch[symbol]['generation'],2)

    async def test_missing_marker_or_partial_snapshot_is_rejected(self):
        self.conn.watch['BADUSDT'] = dict(symbol='BADUSDT',status='PENDING',generation=1)
        with self.assertRaises(ValueError):
            await self.store.seed_policy(load_seed())
        self.conn.watch.clear()
        await self.store.seed_policy(load_seed())
        del self.conn.watch[load_seed()['snapshot'][0]]
        with self.assertRaises(ValueError):
            await self.store.seed_policy(load_seed())

    async def test_changed_seed_marker_rejected_without_overwrite(self):
        await self.store.seed_policy(load_seed())
        before = copy.deepcopy(self.conn.watch)
        marker = json.loads(self.conn.meta[SEED_META_KEY])
        marker['sha256'] = 'incorrect'
        self.conn.meta[SEED_META_KEY] = json.dumps(marker)
        with self.assertRaises(ValueError):
            await self.store.seed_policy(load_seed())
        self.assertEqual(self.conn.watch,before)

    async def test_new_contract_notice_and_state_are_one_transaction(self):
        record = dict(symbol='NEWUSDT', status='PENDING',generation=1,listing_key='listing:1',notice_id='new:NEW:1')
        event = Event(record['notice_id'],'NEWUSDT','NEW',1,'approval',{'inline_keyboard':[[{'text':'Yes','callback_data':'ok'}]]})
        state = to_dict(State('NEWUSDT','NEW',1))
        self.conn.fail = 'INSERT INTO bot_pending'
        with self.assertRaises(RuntimeError):
            await self.store.commit(states=[('NEWUSDT',state)],events=[event],watch=[record])
        self.assertFalse(self.conn.watch or self.conn.states or self.conn.candles or self.conn.pending)
        self.assertFalse(self.store.history_revisions)
        self.conn.fail = ''
        await self.store.commit(states=[('NEWUSDT',state)],events=[event],watch=[record])
        pending = await self.store.first_pending(1)
        self.assertEqual(pending['reply_markup'],event.reply_markup)
        with self.assertRaises(PolicyConflict):
            await self.store.commit(events=[event],watch=[record])
        self.assertEqual(len(self.conn.pending),1)
        await self.store.complete(event.id,message_id=123)
        self.assertFalse(self.conn.pending)
        self.assertEqual(self.conn.watch['NEWUSDT']['message_id'],123)

    async def test_cas_duplicate_callback_and_wrong_status_are_rejected(self):
        pending = dict(symbol='NEWUSDT',status='PENDING',generation=1,notice_id='new:1')
        await self.store.commit(watch=[pending])
        approved = dict(pending,status='APPROVED',generation=2,expected_generation=1,expected_status='PENDING')
        await self.store.commit(watch=[approved])
        with self.assertRaises(PolicyConflict):
            await self.store.commit(watch=[approved])
        with self.assertRaises(PolicyConflict):
            await self.store.commit(watch=[dict(approved,generation=3,expected_generation=2,expected_status='PENDING')])
        self.assertEqual(self.conn.watch['NEWUSDT']['generation'],2)

    async def test_cas_state_rollback_does_not_persist_success(self):
        pending = dict(symbol='NEWUSDT',status='PENDING',generation=1)
        await self.store.commit(watch=[pending])
        self.conn.fail = 'INSERT INTO bot_symbols'
        with self.assertRaises(RuntimeError):
            await self.store.commit(states=[('NEWUSDT',{'history_revision':0})],
                watch=[dict(pending,status='APPROVED',generation=2,expected_generation=1)])
        self.assertEqual(self.conn.watch['NEWUSDT']['status'],'PENDING')
        self.assertEqual(self.conn.watch['NEWUSDT']['generation'],1)

    async def test_only_approved_candle_json_is_loaded_and_old_flags_survive(self):
        approved = State('YESUSDT','YES',1,a_epoch='same',a_sent=True,history_revision=4)
        blocked = State('NOUSDT','NO',1,b_epoch='sameB',b_sent=True,history_revision=7)
        for state in (approved,blocked):
            payload = to_dict(state)
            payload.pop('candles')
            self.conn.states[state.symbol] = json.dumps(payload)
        candle = [{'open_ms':0,'close_ms':14399999,'close':'1'}]
        self.conn.candles['YESUSDT'] = json.dumps(candle)
        # Invalid JSON intentionally proves that nonapproved history is never decoded.
        self.conn.candles['NOUSDT'] = 'must not decode or transfer this history'
        states,meta = await self.store.load_states({'YESUSDT'})
        self.assertEqual(self.conn.cursor_symbols,{'YESUSDT'})
        self.assertEqual(len(states['YESUSDT'].candles),1)
        self.assertEqual(states['NOUSDT'].candles,[])
        self.assertTrue(states['YESUSDT'].a_sent)
        self.assertTrue(states['NOUSDT'].b_sent)
        self.assertEqual(states['NOUSDT'].b_epoch,'sameB')
        self.assertEqual(self.store.history_revisions,{'YESUSDT':4,'NOUSDT':7})
        self.assertEqual(meta,{})

    async def test_metadata_only_writes_never_replace_existing_candles(self):
        self.conn.candles['NOUSDT'] = 'historic JSON preserved byte for byte'
        self.store.history_revisions['NOUSDT'] = 7
        await self.store.commit(states=[('NOUSDT',dict(symbol='NOUSDT',history_revision=99,a_sent=True))])
        self.assertEqual(self.conn.candles['NOUSDT'],'historic JSON preserved byte for byte')
        self.assertEqual(self.store.history_revisions['NOUSDT'],7)
        self.assertFalse(any(sql.startswith('INSERT INTO bot_candles') for sql,args in self.conn.statements))

    async def test_reapprove_reuses_existing_candles_without_database_writes(self):
        value = json.dumps([dict(open_ms=0,close_ms=14399999,close='2')])
        self.conn.candles['YESUSDT'] = value
        loaded = await self.store.load_history('YESUSDT')
        self.assertEqual(loaded,[Candle(0,14399999,Decimal('2'))])
        self.assertEqual(self.conn.candles['YESUSDT'],value)
        self.assertTrue(all(sql.startswith('SELECT') for sql,args in self.conn.statements))

    async def test_unchanged_history_revision_avoids_candle_rewrite(self):
        state = State('YESUSDT','YES',1,history_revision=4)
        await self.store.commit(states=[('YESUSDT',to_dict(state))])
        self.conn.statements.clear()
        await self.store.commit(states=[('YESUSDT',to_dict(state))])
        self.assertFalse(any(sql.startswith('INSERT INTO bot_candles') for sql,args in self.conn.statements))

    async def test_outbox_backpressure_rolls_policy_back(self):
        self.store.max_pending = 0
        with self.assertRaises(Backpressure):
            await self.store.commit(watch=[dict(symbol='NEWUSDT',status='PENDING',generation=1)],
                events=[Event('new','NEWUSDT','NEW',1,'notice')])
        self.assertFalse(self.conn.watch or self.conn.pending)

    async def test_ack_failure_keeps_message_and_notice_marker(self):
        record = dict(symbol='NEWUSDT',status='PENDING',generation=1,notice_id='new:1')
        await self.store.commit(watch=[record],events=[Event('new:1','NEWUSDT','NEW',1,'notice')])
        self.conn.fail = 'DELETE FROM bot_pending'
        with self.assertRaises(RuntimeError):
            await self.store.complete('new:1',message_id=10)
        self.assertIn('new:1',self.conn.pending)
        self.assertIsNone(self.conn.watch['NEWUSDT']['message_id'])


    async def test_stale_cache_cannot_erase_acknowledged_notice_id(self):
        pending = dict(symbol='NEWUSDT',status='PENDING',generation=1,notice_id='new:1',message_id=None)
        await self.store.commit(watch=[pending],events=[Event('new:1','NEWUSDT','NEW',1,'notice')])
        await self.store.complete('new:1',message_id=321)
        # Simulate a command from the Bot cache before sender synchronization.
        approved = dict(pending,status='APPROVED',generation=2,expected_generation=1)
        await self.store.commit(watch=[approved])
        self.assertEqual(self.conn.watch['NEWUSDT']['message_id'],321)
        # A genuinely new request must not inherit the old Telegram message.
        renewed = dict(pending,generation=3,expected_generation=2,notice_id='new:3')
        await self.store.commit(watch=[renewed])
        self.assertIsNone(self.conn.watch['NEWUSDT']['message_id'])
