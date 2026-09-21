"""Notebook-friendly Parquet tables in a private Cloudflare R2 bucket."""
from __future__ import annotations

import io
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import find_dotenv, load_dotenv

PREFIX = 'tables/'
SUMMARY_COLUMNS = ['table', 'created_at', 'updated_at', 'rows', 'columns', 'size_bytes', 'description']


def _connection():
    load_dotenv(find_dotenv(usecwd=True), override=False)
    settings = {
        'R2_ENDPOINT_URL': os.getenv('R2_ENDPOINT_URL') or os.getenv('endpoint'),
        'R2_ACCESS_KEY_ID': os.getenv('R2_ACCESS_KEY_ID') or os.getenv('access_key'),
        'R2_SECRET_ACCESS_KEY': os.getenv('R2_SECRET_ACCESS_KEY') or os.getenv('secret'),
        'R2_BUCKET': os.getenv('R2_BUCKET'),
    }
    missing = [key for key, value in settings.items() if not value]
    if missing:
        raise ValueError('Missing .env settings: ' + ', '.join(missing))
    client = boto3.client(
        's3', endpoint_url=settings['R2_ENDPOINT_URL'], region_name='auto',
        aws_access_key_id=settings['R2_ACCESS_KEY_ID'],
        aws_secret_access_key=settings['R2_SECRET_ACCESS_KEY'],
        config=Config(retries={'mode': 'standard', 'max_attempts': 3},
                      connect_timeout=15, read_timeout=120),
    )
    return client, settings['R2_BUCKET']


def _key(table):
    if not isinstance(table, str) or not re.fullmatch(r'[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*', table):
        raise ValueError('Table names must contain letters, numbers, underscores or hyphens; / separates groups.')
    return PREFIX + table + '.parquet'


def _head(client, bucket, key):
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response['Error']['Code'] in ('404', 'NoSuchKey', 'NotFound'):
            raise FileNotFoundError('Table not found: ' + key) from None
        raise


def _summary(table, head):
    info = json.loads(head.get('Metadata', {}).get('ubctgdb', '{}'))
    modified = head['LastModified'].isoformat()
    return dict(table=table, created_at=info.get('created_at', modified),
                updated_at=modified, rows=info.get('rows'), columns=info.get('columns'),
                size_bytes=head['ContentLength'], description=info.get('description', ''))


class _RemoteFile(io.RawIOBase):
    """Seekable, ETag-pinned range reads for Parquet footers and previews."""
    def __init__(self, client, bucket, key, head):
        self.client, self.bucket, self.key = client, bucket, key
        self.size, self.etag, self.position = head['ContentLength'], head['ETag'], 0

    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.position

    def seek(self, offset, whence=0):
        if whence not in (0, 1, 2):
            raise ValueError('Invalid seek mode')
        position = offset + (0 if whence == 0 else self.position if whence == 1 else self.size)
        if position < 0:
            raise ValueError('Negative seek position')
        self.position = position
        return position

    def read(self, size=-1):
        end = self.size if size is None or size < 0 else min(self.size, self.position + size)
        if end <= self.position:
            return b''
        response = self.client.get_object(
            Bucket=self.bucket, Key=self.key, IfMatch=self.etag,
            Range=f'bytes={self.position}-{end - 1}',
        )
        with response['Body'] as body:
            data = body.read()
        if len(data) != end - self.position:
            raise IOError('Incomplete range download; retry the operation.')
        self.position = end
        return data


def list_tables(*, search=None, sort_by='created_at', ascending=False):
    """Return table summaries, newest additions first. Search matches table names."""
    if sort_by not in SUMMARY_COLUMNS:
        raise ValueError('Unknown sort column: ' + str(sort_by))
    client, bucket = _connection()
    records = []
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.parquet'):
                continue
            name = key[len(PREFIX):-len('.parquet')]
            if search is not None and search.casefold() not in name.casefold():
                continue
            records.append(_summary(name, _head(client, bucket, key)))
    return pd.DataFrame(records, columns=SUMMARY_COLUMNS).sort_values(
        sort_by, ascending=ascending, kind='stable', ignore_index=True,
    )


def describe(table):
    """Return a summary dictionary, including the Parquet column names and types."""
    key = _key(table)
    client, bucket = _connection()
    head = _head(client, bucket, key)
    with _RemoteFile(client, bucket, key, head) as source:
        with pq.ParquetFile(source) as parquet:
            info = _summary(table, head)
            info.update(rows=parquet.metadata.num_rows, columns=len(parquet.schema_arrow),
                        schema={field.name: str(field.type) for field in parquet.schema_arrow})
            return info


def preview(table, rows=20):
    """Return the first 0–1000 rows via range reads; no full local download."""
    if isinstance(rows, bool) or not isinstance(rows, int) or not 0 <= rows <= 1000:
        raise ValueError('rows must be an integer between 0 and 1000')
    key = _key(table)
    client, bucket = _connection()
    with _RemoteFile(client, bucket, key, _head(client, bucket, key)) as source:
        with pq.ParquetFile(source) as parquet:
            if rows:
                batch = next(parquet.iter_batches(batch_size=rows, use_threads=False), None)
                if batch is not None:
                    return batch.to_pandas()
            return parquet.schema_arrow.empty_table().to_pandas()


def download_table(table, destination, *, overwrite=False):
    """Download a complete Parquet file. Existing local files are protected by default."""
    key = _key(table)
    destination = Path(destination).expanduser()
    if destination.exists() and not overwrite:
        raise FileExistsError('Destination exists; pass overwrite=True to replace it.')
    client, bucket = _connection()
    head = _head(client, bucket, key)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(suffix='.partial', dir=destination.parent)
    os.close(fd)
    try:
        client.download_file(bucket, key, temporary)
        after = _head(client, bucket, key)
        if head['ETag'] != after['ETag'] or Path(temporary).stat().st_size != head['ContentLength']:
            raise IOError('Table changed or download was incomplete; retry the operation.')
        with pq.ParquetFile(temporary) as parquet:
            if len(parquet.schema_arrow) == 0:
                raise ValueError('Dataset has no columns')
        if destination.exists() and not overwrite:
            raise FileExistsError('Destination exists; pass overwrite=True to replace it.')
        os.replace(temporary, destination)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return destination


def read_table(table, *, columns=None):
    """Download current data into pandas. No persistent cache; each call fetches anew."""
    with tempfile.TemporaryDirectory(prefix='ubctgdb-') as folder:
        path = download_table(table, Path(folder) / 'data.parquet')
        return pd.read_parquet(path, columns=columns)


def upload_parquet(path, *, table, description=None, replace_table=False):
    """Publish an existing Parquet file. Replacements have no history or undo."""
    key = _key(table)
    path = Path(path).expanduser()
    with pq.ParquetFile(path) as parquet:
        rows, columns = parquet.metadata.num_rows, len(parquet.schema_arrow)
        if not columns:
            raise ValueError('Dataset must have at least one column')
    client, bucket = _connection()
    try:
        previous = _summary(table, _head(client, bucket, key))
    except FileNotFoundError:
        previous = None
    if previous is not None and not replace_table:
        raise FileExistsError('Table exists; pass replace_table=True to replace it.')
    info = dict(
        created_at=previous['created_at'] if previous else datetime.now(timezone.utc).isoformat(),
        description=description if description is not None else previous['description'] if previous else '',
        rows=rows, columns=columns,
    )
    if not isinstance(info['description'], str):
        raise TypeError('description must be a string')
    metadata = json.dumps(info, ensure_ascii=True)
    if len(metadata.encode('ascii')) > 1800:
        raise ValueError('Description is too long for object metadata; use a shorter description.')
    # Data and metadata become visible together after the upload completes.
    client.upload_file(str(path), bucket, key, ExtraArgs={
        'Metadata': {'ubctgdb': metadata}, 'ContentType': 'application/vnd.apache.parquet',
    })
    return _summary(table, _head(client, bucket, key))


def upload_dataframe(df, *, table, description=None, replace_table=False):
    """Publish a DataFrame as Zstandard-compressed Parquet, without its index."""
    _key(table)
    with tempfile.TemporaryDirectory(prefix='ubctgdb-') as folder:
        path = Path(folder) / 'data.parquet'
        df.to_parquet(path, index=False, compression='zstd', row_group_size=10_000)
        return upload_parquet(path, table=table, description=description, replace_table=replace_table)
