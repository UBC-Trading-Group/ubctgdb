"""Small offline suite: real Parquet files with an in-memory storage client."""
import hashlib
import io
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

import pandas as pd
from botocore.exceptions import ClientError

import ubctgdb as db
from ubctgdb import core


class Storage:
    def __init__(self):
        self.meta = SimpleNamespace(endpoint_url='https://test.r2.cloudflarestorage.com')
        self.downloads = 0
        self.objects = {}
        self.fail_download = False
        self.fail_upload = False
        self.fail_copy = False
        self.fail_delete = False
        self.bad_copy = False
        self.multipart = {}

    def upload_file(self, path, bucket, key, ExtraArgs):
        if self.fail_upload:
            raise IOError('Interrupted upload')
        data = Path(path).read_bytes()
        self.objects[key] = (data, dict(
            Metadata=ExtraArgs['Metadata'], ContentLength=len(data),
            ETag=hashlib.sha256(data).hexdigest(), LastModified=datetime.now(timezone.utc),
        ))

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        return self.objects[Key][1]

    def get_paginator(self, name):
        return self

    def paginate(self, Bucket, Prefix):
        # One object per page exercises pagination.
        for key in self.objects:
            if key.startswith(Prefix):
                yield {'Contents': [{'Key': key}]}

    def get_object(self, Bucket, Key, IfMatch, Range):
        data, head = self.objects[Key]
        if IfMatch != head['ETag']:
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'GetObject')
        start, end = map(int, Range.removeprefix('bytes=').split('-'))
        return {'Body': io.BytesIO(data[start:end + 1])}

    def download_file(self, bucket, key, path):
        self.downloads += 1
        data = self.objects[key][0]
        Path(path).write_bytes(data[:10] if self.fail_download else data)
        if self.fail_download:
            raise IOError('Interrupted download')

    def delete_object(self, Bucket, Key):
        if self.fail_delete:
            raise IOError('Delete failed')
        self.objects.pop(Key, None)

    def copy_object(self, Bucket, Key, CopySource, CopySourceIfMatch, MetadataDirective, Metadata, ContentType):
        if self.fail_copy:
            raise IOError('Copy failed')
        data, head = self.objects[CopySource['Key']]
        assert CopySourceIfMatch == head['ETag']
        head = dict(head)
        head['Metadata'] = Metadata
        if self.bad_copy:
            head['ContentLength'] += 1
        self.objects[Key] = (data, head)

    def create_multipart_upload(self, Bucket, Key, Metadata, ContentType):
        self.multipart = {'key': Key, 'metadata': Metadata, 'parts': []}
        return {'UploadId': 'test'}

    def upload_part_copy(self, Bucket, Key, CopySource, UploadId, PartNumber, CopySourceRange):
        if self.fail_copy:
            raise IOError('Part copy failed')
        start, end = map(int, CopySourceRange.removeprefix('bytes=').split('-'))
        self.multipart['parts'].append(self.objects[CopySource['Key']][0][start:end + 1])
        return {'CopyPartResult': {'ETag': str(PartNumber)}}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        data = b''.join(self.multipart['parts'])
        self.objects[Key] = (data, dict(Metadata=self.multipart['metadata'], ContentLength=len(data),
            ETag=hashlib.sha256(data).hexdigest(), LastModified=datetime.now(timezone.utc)))
        self.multipart = {}

    def abort_multipart_upload(self, **kwargs):
        self.multipart = {}


class TablesTest(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        root = patch.object(core._cache, 'ROOT', Path(folder.name))
        root.start()
        self.addCleanup(root.stop)
        self.storage = Storage()
        self.connection = patch.object(core, '_connection', return_value=(self.storage, 'test'))
        self.connection.start()
        self.addCleanup(self.connection.stop)
        self.frame = pd.DataFrame({
            'id': pd.Series([1, None, 3], dtype='Int64'),
            'value': [1.23456789012345, float('nan'), -2.5],
            'label': ['a', None, 'é'],
            'date': pd.to_datetime(['2020-01-01', None, '2020-01-03']),
        })

    def test_round_trip_all_commands(self):
        self.assertTrue(db.list_tables().empty)
        receipt = db.upload_dataframe(self.frame, table='factor/test', description='Example é')
        self.assertEqual(receipt['rows'], 3)
        self.assertEqual(db.list_tables()['table'].tolist(), ['factor/test'])
        self.assertEqual(db.describe('factor/test')['schema']['id'], 'int64')
        pd.testing.assert_frame_equal(db.preview('factor/test', 2), self.frame.head(2))
        pd.testing.assert_frame_equal(db.read_table('factor/test'), self.frame)
        pd.testing.assert_frame_equal(db.read_table('factor/test', columns=['id']), self.frame[['id']])
        with tempfile.TemporaryDirectory() as folder:
            path = db.download_table('factor/test', Path(folder) / 'test.parquet')
            pd.testing.assert_frame_equal(pd.read_parquet(path), self.frame)
            db.upload_parquet(path, table='copy')
            self.assertEqual(db.list_tables()['table'].tolist(), ['copy', 'factor/test'])
            self.assertEqual(db.list_tables(search='FACTOR')['table'].tolist(), ['factor/test'])
            with self.assertRaises(FileExistsError):
                db.download_table('copy', path)

    def test_settings_reload_without_environment_pollution(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / '.env'
            with patch.object(core, 'find_dotenv', return_value=str(path)), patch.dict(os.environ, {}, clear=True):
                path.write_text('YOUR_NAME=Alex\nR2_BUCKET=first\n', encoding='utf-8')
                self.assertEqual(core._settings()['YOUR_NAME'], 'Alex')
                self.assertNotIn('YOUR_NAME', os.environ)
                path.write_text('YOUR_NAME=Sam\nR2_BUCKET=second\n', encoding='utf-8')
                self.assertEqual(core._settings()['YOUR_NAME'], 'Sam')
                self.assertEqual(core._settings()['R2_BUCKET'], 'second')
                path.write_text('R2_BUCKET=second\n', encoding='utf-8')
                self.assertNotIn('YOUR_NAME', core._settings())
                os.environ['R2_BUCKET'] = 'explicit'
                self.assertEqual(core._settings()['R2_BUCKET'], 'explicit')

    def test_corrupt_cache_metadata_recovers(self):
        db.upload_dataframe(self.frame, table='cached')
        for broken in ('{', '{}', '[]', '{"last_used": "bad"}'):
            db.read_table('cached')
            metadata = next(core._cache.ROOT.glob('*.json'))
            metadata.write_text(broken, encoding='utf-8')
            self.assertTrue(db.cache_info().empty)
            self.assertEqual(list(core._cache.ROOT.glob('*.parquet')), [])
            pd.testing.assert_frame_equal(db.read_table('cached'), self.frame)

    def test_cache_cleanup_failure_does_not_hide_remote_success(self):
        with patch.object(core._cache, 'clear', side_effect=PermissionError('locked')):
            with self.assertWarnsRegex(RuntimeWarning, 'R2 change succeeded'):
                receipt = db.upload_dataframe(self.frame, table='cleanup')
            self.assertEqual(receipt['rows'], 3)
            with self.assertWarnsRegex(RuntimeWarning, 'R2 change succeeded'):
                db.rename_table('cleanup', 'cleanup-new')
            pd.testing.assert_frame_equal(db.read_table('cleanup-new'), self.frame)
            with self.assertWarnsRegex(RuntimeWarning, 'R2 change succeeded'):
                self.assertTrue(db.delete_table('cleanup-new')['deleted'])
        self.assertNotIn(core._key('cleanup-new'), self.storage.objects)

    def test_duplicate_rejected_before_serialization(self):
        db.upload_dataframe(self.frame, table='existing')
        with patch.object(pd.DataFrame, 'to_parquet') as serialize:
            with self.assertRaisesRegex(FileExistsError, 'replace_table=True'):
                db.upload_dataframe(self.frame, table='existing')
            serialize.assert_not_called()

    def test_replace_and_empty(self):
        first = db.upload_dataframe(self.frame, table='test', description='Keep me')
        with self.assertRaises(FileExistsError):
            db.upload_dataframe(self.frame, table='test')
        empty = self.frame.iloc[:0]
        second = db.upload_dataframe(empty, table='test', replace_table=True)
        self.assertEqual(first['created_at'], second['created_at'])
        self.assertEqual(second['description'], 'Keep me')
        pd.testing.assert_frame_equal(db.read_table('test'), empty)
        self.assertTrue(db.preview('test').empty)

    def test_updated_by_and_description(self):
        with patch.dict(os.environ, {'YOUR_NAME': 'Alex'}):
            db.upload_dataframe(self.frame, table='credit', description='Practice data.')
        self.assertEqual(db.list_tables().iloc[0]['updated_by'], 'Alex')
        with patch.dict(os.environ, {'YOUR_NAME': 'Sam'}):
            db.upload_dataframe(self.frame, table='credit', replace_table=True)
        self.assertEqual(db.describe('credit')['description'], 'Practice data.')
        self.assertEqual(db.describe('credit')['updated_by'], 'Sam')
        with patch.dict(os.environ, {'YOUR_NAME': 'Jo'}):
            db.rename_table('credit', 'renamed-credit')
        self.assertEqual(db.describe('renamed-credit')['updated_by'], 'Jo')
        self.assertEqual(db.describe('renamed-credit')['description'], 'Practice data.')
        with patch.dict(os.environ, {'YOUR_NAME': ''}), patch.object(core, '_COPY_LIMIT', 1):
            db.rename_table('renamed-credit', 'anonymous')
            db.upload_dataframe(self.frame, table='anonymous', replace_table=True, description='')
        self.assertEqual(db.describe('anonymous')['updated_by'], '')
        self.assertEqual(db.describe('anonymous')['description'], '')

    def test_failures_preserve_data_and_cleanup(self):
        db.upload_dataframe(self.frame, table='test')
        self.storage.fail_upload = True
        with self.assertRaises(IOError):
            db.upload_dataframe(self.frame.iloc[:1], table='test', replace_table=True)
        pd.testing.assert_frame_equal(db.read_table('test'), self.frame)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'existing.parquet'
            path.write_bytes(b'original')
            self.storage.fail_download = True
            with self.assertRaises(IOError):
                db.download_table('test', path, overwrite=True)
            self.assertEqual(path.read_bytes(), b'original')
            self.assertEqual(list(Path(folder).iterdir()), [path])
        with self.assertRaises(FileNotFoundError):
            db.describe('missing')
        with self.assertRaises(ValueError):
            db.preview('test', -1)
        with self.assertRaises(ValueError):
            db.read_table('../outside')

    def test_rename_and_delete(self):
        first = db.upload_dataframe(self.frame, table='old', description='Keep metadata')
        db.upload_dataframe(self.frame, table='occupied')
        with self.assertRaises(FileExistsError):
            db.rename_table('old', 'occupied')
        with self.assertRaises(ValueError):
            db.rename_table('old', 'old')
        self.assertEqual(db.rename_table('old', 'new'), {'old_name': 'old', 'new_name': 'new'})
        info = db.describe('new')
        self.assertEqual(info['created_at'], first['created_at'])
        self.assertEqual(info['description'], first['description'])
        pd.testing.assert_frame_equal(db.read_table('new'), self.frame)
        with self.assertRaises(FileNotFoundError):
            db.describe('old')
        self.assertEqual(db.delete_table('new'), {'table': 'new', 'deleted': True})
        with self.assertRaises(FileNotFoundError):
            db.delete_table('new')

    def test_rename_failures_retain_original(self):
        db.upload_dataframe(self.frame, table='old')
        for flag, error in [('fail_copy', IOError), ('bad_copy', IOError), ('fail_delete', RuntimeError)]:
            with self.subTest(flag=flag):
                setattr(self.storage, flag, True)
                with self.assertRaises(error):
                    db.rename_table('old', 'new')
                setattr(self.storage, flag, False)
                pd.testing.assert_frame_equal(db.read_table('old'), self.frame)
                if flag == 'fail_delete':
                    pd.testing.assert_frame_equal(db.read_table('new'), self.frame)
                self.storage.objects.pop('tables/new.parquet', None)

    def test_multipart_rename_and_abort(self):
        db.upload_dataframe(self.frame, table='old', description='Multipart')
        with patch.object(core, '_COPY_LIMIT', 1), patch.object(core, '_COPY_PART_SIZE', 1000):
            self.storage.fail_copy = True
            with self.assertRaises(IOError):
                db.rename_table('old', 'new')
            self.assertEqual(self.storage.multipart, {})
            self.storage.fail_copy = False
            db.rename_table('old', 'new')
        pd.testing.assert_frame_equal(db.read_table('new'), self.frame)
        self.assertEqual(db.describe('new')['description'], 'Multipart')

    def test_cache_freshness_refresh_and_isolation(self):
        db.upload_dataframe(self.frame, table='test')
        first = db.read_table('test')
        first.loc[0, 'value'] = 999
        pd.testing.assert_frame_equal(db.read_table('test'), self.frame)
        self.assertEqual(self.storage.downloads, 1)
        db.read_table('test', refresh=True)
        self.assertEqual(self.storage.downloads, 2)
        self.assertEqual(db.cache_info()['table'].tolist(), ['test'])
        # Simulate a replacement from another machine, without local invalidation.
        with patch.object(core._cache, 'clear'):
            db.upload_dataframe(self.frame.iloc[:1], table='test', replace_table=True)
        self.assertEqual(len(db.read_table('test')), 1)
        self.assertEqual(self.storage.downloads, 3)
        with patch.object(self.storage, 'head_object', side_effect=IOError('offline')):
            with self.assertRaises(IOError):
                db.read_table('test')
        self.storage.meta.endpoint_url = 'https://other.r2.cloudflarestorage.com'
        self.assertTrue(db.cache_info().empty)
        db.read_table('test')
        self.assertEqual(self.storage.downloads, 4)
        self.assertEqual(db.clear_cache(), 1)
        self.storage.meta.endpoint_url = 'https://test.r2.cloudflarestorage.com'
        self.assertEqual(db.clear_cache('test'), 1)
        self.assertEqual(db.describe('test')['rows'], 1)

    def test_cache_limit_concurrency_and_invalidation(self):
        from concurrent.futures import ThreadPoolExecutor
        db.upload_dataframe(self.frame, table='one')
        db.upload_dataframe(self.frame, table='two')
        size = db.describe('one')['size_bytes']
        with patch.object(core._cache, 'MAX_BYTES', size):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(db.read_table, ['one', 'one']))
            self.assertEqual(self.storage.downloads, 1)
            pd.testing.assert_frame_equal(results[0], results[1])
            db.read_table('two')
            self.assertEqual(db.cache_info()['table'].tolist(), ['two'])
        with patch.object(core._cache, 'MAX_BYTES', 1):
            db.read_table('one')
            self.assertTrue(db.cache_info().empty)
        db.read_table('one')
        db.upload_dataframe(self.frame, table='one', replace_table=True)
        self.assertTrue(db.cache_info().empty)
        db.read_table('one')
        db.rename_table('one', 'renamed')
        self.assertTrue(db.cache_info().empty)
        db.read_table('renamed')
        db.delete_table('renamed')
        self.assertTrue(db.cache_info().empty)
        self.storage.fail_download = True
        with self.assertRaises(IOError):
            db.read_table('two')
        self.assertTrue(db.cache_info().empty)
        self.assertEqual(list(core._cache.ROOT.glob('*.partial')), [])

    def test_folder_prefixes(self):
        for name in ['raw/prices/september', 'raw/prices/october', 'raw/info', 'raw_backup/prices', 'top']:
            db.upload_dataframe(self.frame, table=name)
        self.assertEqual(len(db.list_tables()), 5)
        self.assertEqual(len(db.list_tables(folder='raw/')), 3)
        result = db.list_tables(folder='raw/prices', search='september')
        self.assertEqual(result['table'].tolist(), ['raw/prices/september'])
        self.assertTrue(db.list_tables(folder='missing').empty)
        with self.assertRaises(ValueError):
            db.list_tables(folder='../raw')


if __name__ == '__main__':
    unittest.main()
