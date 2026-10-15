"""Offline, isolated RSS comparison of deployed v3 and selective-loading v4.

No network, PostgreSQL or Telegram connection is opened. Run with python -B
memory_benchmark.py. Includes installed aiohttp/asyncpg and the current app's
runtime imports. Each version runs in a fresh subprocess; v3 source is read from
the fixed pre-v4 Git commit in memory; --baseline COMMIT can override it. The fake SQL driver generates persisted JSON one row at a time.
"""
from __future__ import annotations
import argparse
import asyncio
import ctypes
import gc
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import types

ROOT = Path(__file__).resolve().parent
BASELINE_DEFAULT = '7dad34c4756e32fcde5d1fa927379ec1b8e92978'
GIT = os.environ.get('GIT_EXECUTABLE', 'git')
NOW = 1_800_000_000_000
FOUR_HOURS_MS = 14_400_000


def memory_bytes():
    if sys.platform == 'win32':
        from ctypes import wintypes
        class Counters(ctypes.Structure):
            _fields_ = [('cb', wintypes.DWORD), ('PageFaultCount', wintypes.DWORD),
                        ('PeakWorkingSetSize', ctypes.c_size_t), ('WorkingSetSize', ctypes.c_size_t),
                        ('QuotaPeakPagedPoolUsage', ctypes.c_size_t), ('QuotaPagedPoolUsage', ctypes.c_size_t),
                        ('QuotaPeakNonPagedPoolUsage', ctypes.c_size_t), ('QuotaNonPagedPoolUsage', ctypes.c_size_t),
                        ('PagefileUsage', ctypes.c_size_t), ('PeakPagefileUsage', ctypes.c_size_t)]
        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        handle = ctypes.windll.kernel32.GetCurrentProcess
        handle.restype = wintypes.HANDLE
        get_info = ctypes.windll.psapi.GetProcessMemoryInfo
        get_info.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        get_info.restype = wintypes.BOOL
        if not get_info(handle(), ctypes.byref(counters), counters.cb):
            raise ctypes.WinError()
        return {'rss': counters.WorkingSetSize, 'peak': counters.PeakWorkingSetSize}
    import resource
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak = peak if sys.platform == 'darwin' else peak * 1024
    try:
        rss = int(Path('/proc/self/statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')
    except (OSError, IndexError):
        rss = peak
    return {'rss': rss, 'peak': peak}


def source_at(revision, filename):
    git = GIT if Path(GIT).exists() else 'git'
    return subprocess.check_output([git, 'show', f'{revision}:{filename}'], cwd=ROOT).decode('utf-8')


def module_source(name, source):
    module = types.ModuleType(name)
    module.__file__ = str(ROOT / (name + '.py'))
    sys.modules[name] = module
    exec(compile(source, module.__file__, 'exec'), module.__dict__)
    return module


async def child(mode, baseline):
    # Import actual runtime dependencies even though no connection is made.
    import aiohttp
    import asyncpg
    import app
    import engine
    import storage
    policy = json.loads((ROOT / 'approval_policy.json').read_text(encoding='utf-8-sig'))
    symbols = policy['snapshot']
    approved = set(policy['approved'])
    current_engine, current_store = engine, storage
    if mode == 'v3':
        current_engine = module_source('_benchmark_engine_v3', source_at(baseline, 'engine.py'))
        current_store = module_source('_benchmark_storage_v3', source_at(baseline, 'storage.py'))
    gc.collect()
    samples = [{'phase': 'startup', **memory_bytes()}]
    row_count = 0
    candle_rows = 0
    threshold = {131, 262, 393, 525}

    def record(phase):
        samples.append({'phase': phase, **memory_bytes()})

    def row(index, include):
        nonlocal row_count, candle_rows
        symbol = symbols[index]
        payload = current_engine.to_dict(current_engine.State(symbol, symbol[:-4], NOW - 100 * 86_400_000))
        payload.pop('candles', None)
        payload.update(name='Offline sample ' + symbol, history_ready=True, history_revision=1,
                       price=str(3_000 + index) + '.12345678', price_ms=NOW,
                       funding='-0.02000000', funding_ms=NOW, interval_hours=4,
                       interval_ms=NOW, floor='-0.02000000', floor_ms=NOW)
        candles = None
        if include:
            first = NOW // FOUR_HOURS_MS * FOUR_HOURS_MS - 541 * FOUR_HOURS_MS
            # Unique Decimal text per candle avoids measuring unrealistically
            # shared price/timestamp objects across the universe.
            candles = json.dumps([{'open_ms': first + j * FOUR_HOURS_MS,
                                   'close_ms': first + (j + 1) * FOUR_HOURS_MS - 1,
                                   'close': str((index + 1) * 1000 + j) + '.12345678'}
                                  for j in range(541)], separators=(',', ':'))
            candle_rows += 1
        row_count += 1
        return {'symbol': symbol, 'state': json.dumps(payload, separators=(',', ':')), 'candles': candles}

    class Transaction:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False

    class Connection:
        def transaction(self):
            return Transaction()
        async def fetch(self, query, *args):
            if 'bot_meta' in query:
                return []
            rows = []
            for index in range(len(symbols)):
                rows.append(row(index, True))
                if index + 1 in threshold:
                    record('loading_json_' + str(index + 1))
            return rows
        def cursor(self, query, selected, prefetch):
            if 'ANY($1::text[])' not in query or prefetch > 16:
                raise AssertionError('v4 must select approved candle rows in SQL with bounded prefetch')
            selected = set(selected)
            async def rows():
                for index, symbol in enumerate(symbols):
                    yield row(index, symbol in selected)
                    if index + 1 in threshold:
                        record('streaming_' + str(index + 1))
            return rows()

    store = current_store.Store('offline://not-connected')
    store.conn = Connection()
    if mode == 'v3':
        raw, _ = await store.load()
        record('all_decoded_json')
        states = {}
        for index, (symbol, payload) in enumerate(raw.items()):
            states[symbol] = current_engine.from_dict(payload)
            if index + 1 in threshold:
                record('constructing_states_' + str(index + 1))
        del raw, payload
    else:
        states, _ = await store.load_states(approved)
    record('collection_complete')
    # Conservative supplement cache: the runtime bounds each 30-day funding
    # history to 750 records. Include all history-bearing symbols, even where
    # an official floor may make supplementation unnecessary in practice.
    funding_histories = {}
    funding_symbols = symbols if mode == 'v3' else [symbol for symbol in symbols if symbol in approved]
    for index, symbol in enumerate(funding_symbols):
        funding_histories[symbol] = json.loads(json.dumps([
            {'symbol': symbol, 'fundingTime': NOW - (749-j) * 3_600_000,
             'fundingRate': '-0.' + str(100000 + index*751+j),
             'markPrice': str((index+1)*1000+j) + '.12345678'}
            for j in range(750)], separators=(',', ':')))
        if index + 1 in (len(funding_symbols)//2, len(funding_symbols)):
            record('funding_cache_' + str(index + 1))
    # One full-universe mark frame is received by the real service even when
    # only approved symbols are evaluated. Keep one decoded frame temporarily.
    frame = json.loads(json.dumps([{'e': 'markPriceUpdate', 'E': NOW,
                                   's': symbol, 'p': str(3000+i)+'.12345678',
                                   'r': '-0.0001', 'T': NOW+3_600_000}
                                  for i, symbol in enumerate(symbols)]))
    record('websocket_frame')
    del frame
    gc.collect()
    record('stable')
    count = sum(len(state.candles) for state in states.values())
    expected = (525 if mode == 'v3' else 337) * 541
    assert len(states) == 525 and count == expected
    assert candle_rows == (525 if mode == 'v3' else 337)
    if mode == 'v4':
        assert all(not state.candles for symbol, state in states.items() if symbol not in approved)
    result = {'version': mode, 'symbols': len(states), 'history_symbols': candle_rows,
              'candle_count': count, 'max_candles_per_symbol': 541,
              'funding_record_count': sum(map(len, funding_histories.values())),
              'samples': samples, 'maximum_peak_bytes': max(item['peak'] for item in samples),
              'runtime': {'python': sys.version.split()[0], 'aiohttp': aiohttp.__version__,
                          'asyncpg': asyncpg.__version__, 'platform': sys.platform}}
    print(json.dumps(result, separators=(',', ':')))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', default=BASELINE_DEFAULT)
    parser.add_argument('--child', choices=('v3', 'v4'))
    args = parser.parse_args()
    if args.child:
        asyncio.run(child(args.child, args.baseline))
        return
    results = []
    for mode in ('v3', 'v4'):
        process = subprocess.run([sys.executable, '-B', str(Path(__file__).resolve()),
                                  '--baseline', args.baseline, '--child', mode],
                                 cwd=ROOT, env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'},
                                 capture_output=True, text=True)
        if process.returncode:
            raise RuntimeError(process.stderr)
        results.append(json.loads(process.stdout))
    before, after = results
    for result in results:
        print(f"{result['version']}: {result['symbols']} states; {result['history_symbols']} histories; "
              f"{result['candle_count']} candles; max {result['max_candles_per_symbol']}/symbol; "
              f"{result['funding_record_count']} cached funding records")
        for sample in result['samples']:
            print(f"  {sample['phase']}: RSS {sample['rss']/2**20:.2f} MiB; peak {sample['peak']/2**20:.2f} MiB")
    reduction = before['maximum_peak_bytes'] - after['maximum_peak_bytes']
    print(f"Peak reduction: {reduction/2**20:.2f} MiB ({100*reduction/before['maximum_peak_bytes']:.1f}%)")
    print('Offline synthetic persisted-JSON load; no production or 24-hour memory guarantee.')
    print('RESULT_JSON=' + json.dumps({'baseline': args.baseline, 'measurements': results}, separators=(',', ':')))


if __name__ == '__main__':
    main()
