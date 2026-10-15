"""Validated, immutable deployment seed; current choices live in PostgreSQL."""
import hashlib
import json
from pathlib import Path

STATUSES = frozenset({'APPROVED', 'BLOCKED', 'REVIEW', 'PENDING'})
SEED_META_KEY = 'approval_seed_v4'
EXPECTED_SEED_SHA256 = '1035c91a3e593814994e88584e65e5c00eafd6bc58a22a083d2e12eac999bcaa'


def validate_seed(config):
    if not isinstance(config, dict) or config.get('version') != 'v4-20261015':
        raise ValueError('Unknown approval seed version')
    groups = {}
    for key, count in (('snapshot', 525), ('approved', 337), ('blocked', 187), ('review', 1)):
        values = config.get(key)
        if (not isinstance(values, list) or len(values) != count
                or any(not isinstance(symbol, str) or not symbol.endswith('USDT')
                       or not symbol[:-4] or not symbol.isalnum() for symbol in values)
                or len(set(values)) != count):
            raise ValueError('Invalid or duplicate approval symbols: ' + key)
        groups[key] = set(values)
    if groups['review'] != {'1000RATSUSDT'}:
        raise ValueError('The confirmed review contract must remain explicit')
    if (groups['approved'] & groups['blocked'] or groups['approved'] & groups['review']
            or groups['blocked'] & groups['review']
            or groups['approved'] | groups['blocked'] | groups['review'] != groups['snapshot']):
        raise ValueError('Approval partitions do not match the trusted 525 snapshot')
    if (type(config.get('snapshot_ms')) is not int or config['snapshot_ms'] <= 0
            or not isinstance(config.get('source'), str) or not config['source']
            or not isinstance(config.get('source_sha256'), str)
            or len(config['source_sha256']) != 64
            or any(c not in '0123456789abcdef' for c in config['source_sha256'])):
        raise ValueError('Missing approval snapshot provenance')
    validated = {key: config[key] for key in ('version', 'snapshot_ms', 'source', 'source_sha256')}
    validated.update({key: sorted(groups[key]) for key in groups})
    canonical = json.dumps(validated, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    validated['sha256'] = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
    if validated['sha256'] != EXPECTED_SEED_SHA256:
        raise ValueError('Approval seed differs from the user-confirmed snapshot')
    return validated


def load_seed(path=None):
    source = Path(path) if path is not None else Path(__file__).with_name('approval_policy.json')
    with source.open(encoding='utf-8-sig') as handle:
        return validate_seed(json.load(handle))
