"""Opt-in integration check: python tests/live_r2.py --run-live.

Uses the configured bucket but only writes newly reserved UUID-named objects.
Run from a directory whose .env configures R2. Never runs under test discovery.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid
from urllib.parse import unquote
from unittest.mock import patch

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

import ubctgdb as db
from ubctgdb import core


def expect(error, call):
    try:
        call()
    except error:
        return
    raise AssertionError('Expected ' + error.__name__)


def child(table, cache_root, log):
    core._cache.ROOT = Path(cache_root)
    client, bucket = core._connection()

    def observe(params, model, **kwargs):
        assert model.name in ('HeadObject', 'GetObject'), model.name
        assert params['Bucket'] == bucket and params['Key'] == core._key(table)
        if model.name == 'GetObject':
            with open(log, 'a', encoding='utf-8') as handle:
                handle.write('GET\n')

    client.meta.events.register('before-parameter-build.s3', observe)
    with patch.object(core, '_connection', return_value=(client, bucket)):
        assert len(db.read_table(table)) == 3


def run():
    client, bucket = core._connection()
    external, external_bucket = core._connection()
    assert bucket == external_bucket
    prefix = 'integration-test-' + uuid.uuid4().hex
    names = [prefix + '/' + name for name in
             ('data', 'nested/data', 'renamed', 'large', 'large-renamed')]
    names.append(prefix + '-other/data')
    allowed = {core._key(name) for name in names}
    counts = Counter()

    def snapshot():
        result = {}
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket):
            for obj in page.get('Contents', []):
                if obj['Key'] not in allowed:
                    head = client.head_object(Bucket=bucket, Key=obj['Key'])
                    result[obj['Key']] = {field: head.get(field) for field in
                        ('ETag', 'ContentLength', 'LastModified', 'Metadata')}
        return result

    before = snapshot()
    # Explicitly establish that every possible destination is absent before writes.
    for name in names:
        expect(FileNotFoundError, lambda name=name: core._head(client, bucket, core._key(name)))

    reads = {'ListObjectsV2', 'HeadObject', 'GetObject'}
    writes = {'PutObject', 'CopyObject', 'DeleteObject', 'CreateMultipartUpload',
              'UploadPart', 'UploadPartCopy', 'CompleteMultipartUpload', 'AbortMultipartUpload'}

    def guard(params, model, **kwargs):
        op = model.name
        assert op in reads | writes, 'Unexpected API operation: ' + op
        assert params['Bucket'] == bucket
        if op in writes:
            assert params['Key'] in allowed, 'Write outside test objects blocked'
            if 'CopySource' in params:
                source = params['CopySource']
                if isinstance(source, dict):
                    assert source['Bucket'] == bucket and source['Key'] in allowed
                else:
                    assert unquote(source).lstrip('/') in {bucket + '/' + key for key in allowed}
        counts[op] += 1

    for connection in (client, external):
        connection.meta.events.register('before-parameter-build.s3', guard)
    print('Test folder:', prefix, flush=True)
    print('Installed package:', db.__file__, flush=True)
    passed = []

    def ok(label):
        passed.append(label)
        print('PASS:', label, flush=True)

    frame = pd.DataFrame({'id': pd.Series([1, None, 3], dtype='Int64'),
                          'value': [1.5, float('nan'), 3.5],
                          'symbol': pd.Series(['AAA', None, 'CCC'], dtype='string'),
                          'date': pd.to_datetime(['2026-01-01', None, '2026-01-03'], utc=True)})
    cleanup_errors = []
    try:
        with tempfile.TemporaryDirectory(prefix='ubctgdb-live-') as temp:
            root = Path(temp)
            with patch.object(core._cache, 'ROOT', root / 'cache'), \
                 patch.object(core, '_connection', return_value=(client, bucket)):
                original = db.upload_dataframe(frame, table=names[0])
                assert_frame_equal(db.read_table(names[0]), frame)
                ok('upload/read preserves values, dtypes and nulls')

                db.upload_dataframe(frame, table=names[1])
                db.upload_dataframe(frame, table=names[-1])
                listing = db.list_tables(folder=prefix)
                assert set(listing.table) == set(names[:2])
                assert set(db.list_tables(folder=prefix + '/').table) == set(names[:2])
                assert set(db.list_tables(folder=prefix + '/nested').table) == {names[1]}
                assert set(names[:2]).issubset(set(db.list_tables().table))
                assert list(db.list_tables(folder=prefix, search='nested').table) == [names[1]]
                for row in listing.to_dict('records'):
                    head = core._head(client, bucket, core._key(row['table']))
                    assert row['size_bytes'] == head['ContentLength']
                    assert row['updated_at'] == head['LastModified'].isoformat()
                    assert row['rows'] == 3 and row['columns'] == 4
                ok('listing metadata, all folders, nested folders and prefix boundaries')

                db.clear_cache()
                with patch.object(client, 'download_file', wraps=client.download_file) as downloads:
                    first = db.read_table(names[0])
                    first.loc[0, 'value'] = 999
                    assert_frame_equal(db.read_table(names[0]), frame)
                    assert downloads.call_count == 1
                    db.read_table(names[0], refresh=True)
                    assert downloads.call_count == 2
                assert list(db.cache_info().table) == [names[0]]
                assert db.clear_cache(names[0]) == 1
                assert db.cache_info().empty
                core._head(client, bucket, core._key(names[0]))
                ok('cache hit, forced refresh, independent frames and safe cache clearing')

                assert_frame_equal(db.preview(names[0], rows=2), frame.head(2))
                assert set(db.describe(names[0])['schema']) == set(frame.columns)
                path = db.download_table(names[0], root / 'download.parquet')
                assert_frame_equal(pd.read_parquet(path), frame)
                expect(FileExistsError, lambda: db.download_table(names[0], path))
                db.download_table(names[0], path, overwrite=True)
                expect(FileExistsError, lambda: db.upload_dataframe(frame, table=names[0]))
                expect(FileExistsError, lambda: db.rename_table(names[0], names[1]))
                expect(ValueError, lambda: db.read_table('../bad'))
                ok('preview, describe, download and expected user errors')

                db.read_table(names[0])
                changed = frame.copy()
                changed.loc[0, 'value'] = 42
                time.sleep(1.1)  # R2 LastModified has second precision.
                replacement = db.upload_dataframe(changed, table=names[0], replace_table=True)
                assert names[0] not in set(db.cache_info().table)
                assert replacement['created_at'] == original['created_at']
                assert replacement['updated_at'] > original['updated_at']
                assert_frame_equal(db.read_table(names[0]), changed)
                # Bypass local invalidation, as a writer on another computer would.
                changed.loc[0, 'value'] = 84
                remote_file = root / 'external.parquet'
                changed.to_parquet(remote_file, index=False, compression='zstd')
                metadata = core._head(external, bucket, core._key(names[0]))['Metadata']
                external.upload_file(str(remote_file), bucket, core._key(names[0]),
                                     ExtraArgs={'Metadata': metadata})
                with patch.object(client, 'download_file', wraps=client.download_file) as downloads:
                    assert_frame_equal(db.read_table(names[0]), changed)
                    assert downloads.call_count == 1
                ok('replacement metadata, local invalidation and external change detection')

                db.rename_table(names[0], names[2])
                expect(FileNotFoundError, lambda: db.read_table(names[0]))
                assert_frame_equal(db.read_table(names[2]), changed)
                assert names[0] not in set(db.cache_info().table)
                db.delete_table(names[2])
                assert names[2] not in set(db.cache_info().table)
                expect(FileNotFoundError, lambda: db.read_table(names[2]))
                ok('rename/delete preserves data and invalidates cached names')

                # A small limit exercises eviction without downloading gigabytes.
                db.clear_cache()
                size = core._head(client, bucket, core._key(names[1]))['ContentLength']
                with patch.object(core._cache, 'MAX_BYTES', size + 1):
                    db.read_table(names[1])
                    db.read_table(names[-1])
                    assert list(db.cache_info().table) == [names[-1]]
                db.clear_cache()
                with patch.object(core._cache, 'MAX_BYTES', 1):
                    assert_frame_equal(db.read_table(names[1]), frame)
                    assert db.cache_info().empty
                ok('cache eviction and oversized-file bypass')

                rng = np.random.default_rng(42)
                large = pd.DataFrame(rng.standard_normal((300_000, 6)))
                large.columns = ['value_' + str(i) for i in range(6)]
                count_before = counts.copy()
                large_summary = db.upload_dataframe(large, table=names[3])
                assert 8 * 1024**2 < large_summary['size_bytes'] < 25 * 1024**2
                assert counts['UploadPart'] - count_before['UploadPart'] >= 2
                assert_frame_equal(db.read_table(names[3]), large)
                assert counts['GetObject'] - count_before['GetObject'] >= 2
                # Force the >5 GB copy path on this modest test file.
                with patch.object(core, '_COPY_LIMIT', 1), \
                     patch.object(core, '_COPY_PART_SIZE', 5 * 1024**2):
                    db.rename_table(names[3], names[4])
                assert counts['UploadPartCopy'] - count_before['UploadPartCopy'] >= 2
                assert_frame_equal(db.read_table(names[4]), large)
                ok('real multipart upload/download and forced multipart rename')

                log = root / 'child-get.log'
                args = [sys.executable, str(Path(__file__).resolve()), '--child', names[1],
                        str(root / 'shared-cache'), str(log)]
                with ThreadPoolExecutor(max_workers=2) as pool:
                    runs = list(pool.map(lambda _: subprocess.run(args, capture_output=True,
                                     text=True, timeout=90), range(2)))
                for result in runs:
                    assert result.returncode == 0, result.stderr
                assert log.read_text(encoding='utf-8').splitlines() == ['GET']
                ok('two independent processes share one real download')
    finally:
        # Exact allowlist only, never a bulk/prefix delete of discovered objects.
        for key in sorted(allowed):
            try:
                client.delete_object(Bucket=bucket, Key=key)
            except Exception as exc:
                cleanup_errors.append(key + ': ' + type(exc).__name__)
        for name in names:
            try:
                core._head(client, bucket, core._key(name))
            except FileNotFoundError:
                pass
            else:
                cleanup_errors.append('Still present: ' + name)
        after = snapshot()
        unchanged = before == after
        print('Existing objects unchanged:', unchanged, '(' + str(len(before)) + ' objects)', flush=True)
        print('Cleanup errors:', cleanup_errors, flush=True)
        print('API request counts:', json.dumps(dict(counts), sort_keys=True), flush=True)
        assert unchanged, 'Existing object snapshot changed; investigate concurrent activity'
        assert not cleanup_errors, cleanup_errors
    print('SUCCESS:', len(passed), 'live test groups passed; all test objects removed.', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-live', action='store_true')
    parser.add_argument('--child', nargs=3, metavar=('TABLE', 'CACHE', 'LOG'))
    args = parser.parse_args()
    if args.child:
        child(*args.child)
    elif args.run_live:
        run()
    else:
        parser.error('Use --run-live to explicitly enable real R2 tests')
