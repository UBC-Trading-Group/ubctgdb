"""Small offline suite: real Parquet files with an in-memory storage client."""
import hashlib
import io
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from botocore.exceptions import ClientError

import ubctgdb as db
from ubctgdb import core


class Storage:
    def __init__(self):
        self.objects = {}
        self.fail_download = False
        self.fail_upload = False

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
        data = self.objects[key][0]
        Path(path).write_bytes(data[:10] if self.fail_download else data)
        if self.fail_download:
            raise IOError('Interrupted download')


class TablesTest(unittest.TestCase):
    def setUp(self):
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


if __name__ == '__main__':
    unittest.main()
