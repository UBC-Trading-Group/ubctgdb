"""A bounded disk cache shared by notebooks, guarded by one process-safe lock."""
import hashlib
import json
import os
import tempfile
import time
from pathlib import Path

import pandas as pd
from filelock import FileLock

MAX_BYTES = 10_000_000_000
ROOT = Path.home() / '.cache' / 'ubctgdb'


def scope(client, bucket):
    return client.meta.endpoint_url.rstrip('/') + '/' + bucket


def _lock():
    ROOT.mkdir(parents=True, exist_ok=True)
    return FileLock(str(ROOT / 'cache.lock'))


def _paths(scope, table):
    name = hashlib.sha256((scope + '\n' + table).encode()).hexdigest()
    return ROOT / (name + '.parquet'), ROOT / (name + '.json')


def _entries():
    for meta in ROOT.glob('*.json'):
        try:
            info = json.loads(meta.read_text(encoding='utf-8'))
            data = meta.with_suffix('.parquet')
            if data.exists():
                yield data, meta, info
        except (ValueError, OSError):
            continue


def _remove(data, meta):
    meta.unlink(missing_ok=True)
    data.unlink(missing_ok=True)


def _prune(reserve=0):
    # The global lock makes abandoned temporary files safe to remove after a crash.
    for path in ROOT.glob('*.partial'):
        path.unlink(missing_ok=True)
    for path in ROOT.glob('*.parquet'):
        if not path.with_suffix('.json').exists():
            path.unlink(missing_ok=True)
    entries = sorted(_entries(), key=lambda entry: entry[2]['last_used'])
    total = sum(data.stat().st_size for data, _, _ in entries)
    for data, meta, _ in entries:
        if total + reserve <= MAX_BYTES:
            break
        total -= data.stat().st_size
        _remove(data, meta)


def read(scope, table, head, download, columns, refresh):
    # Keep the lock through pandas loading so another notebook cannot evict this file.
    with _lock():
        _prune()
        current = head()  # Always check R2, even on a hit. No silent offline fallback.
        data, meta = _paths(scope, table)
        try:
            info = json.loads(meta.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            info = {}
        valid = (not refresh and data.exists() and info.get('etag') == current['ETag']
                 and data.stat().st_size == current['ContentLength'])
        if not valid:
            if current['ContentLength'] > MAX_BYTES:
                _remove(data, meta)
                with tempfile.TemporaryDirectory(prefix='ubctgdb-') as folder:
                    path = Path(folder) / 'data.parquet'
                    download(path, current)
                    return pd.read_parquet(path, columns=columns)
            _remove(data, meta)
            _prune(reserve=current['ContentLength'])
            download(data, current)
        try:
            result = pd.read_parquet(data, columns=columns)
            info = dict(scope=scope, table=table, etag=current['ETag'],
                        size_bytes=current['ContentLength'], last_used=time.time())
            temporary = meta.with_suffix('.json.partial')
            temporary.write_text(json.dumps(info), encoding='utf-8')
            os.replace(temporary, meta)
            return result
        except Exception:
            _remove(data, meta)
            raise


def info(scope):
    with _lock():
        records = [dict(table=i['table'], size_bytes=d.stat().st_size,
                        last_used=pd.to_datetime(i['last_used'], unit='s', utc=True))
                   for d, _, i in _entries() if i['scope'] == scope]
        return pd.DataFrame(records, columns=['table', 'size_bytes', 'last_used']).sort_values(
            'last_used', ascending=False, ignore_index=True)


def clear(scope, table=None):
    with _lock():
        removed = 0
        for data, meta, info in list(_entries()):
            if info['scope'] == scope and (table is None or info['table'] == table):
                _remove(data, meta)
                removed += 1
        return removed
