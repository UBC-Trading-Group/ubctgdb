"""Notebook-friendly Parquet tables in a private Cloudflare R2 bucket."""
from __future__ import annotations

import io
import json
import os
import re
import tempfile
import warnings
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import dotenv_values, find_dotenv
from . import cache as _cache

PREFIX = 'tables/'
_COPY_LIMIT = 5 * 1024**3
_COPY_PART_SIZE = 64 * 1024**2
SUMMARY_COLUMNS = ['table', 'created_at', 'updated_at', 'rows', 'columns', 'size_bytes', 'description', 'updated_by']


def _settings():
    # Read afresh without leaving old .env values in the notebook environment.
    values = dict(dotenv_values(find_dotenv(usecwd=True)))
    values.update(os.environ)
    return values


def _connection():
    values = _settings()
    settings = {
        'R2_ENDPOINT_URL': values.get('R2_ENDPOINT_URL') or values.get('endpoint'),
        'R2_ACCESS_KEY_ID': values.get('R2_ACCESS_KEY_ID') or values.get('access_key'),
        'R2_SECRET_ACCESS_KEY': values.get('R2_SECRET_ACCESS_KEY') or values.get('secret'),
        'R2_BUCKET': values.get('R2_BUCKET'),
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
                size_bytes=head['ContentLength'], description=info.get('description', ''),
                updated_by=info.get('updated_by', ''))


def _metadata(info):
    encoded = json.dumps(info, ensure_ascii=True)
    if len(encoded.encode('ascii')) > 1800:
        raise ValueError('Table metadata is too long; shorten the description or YOUR_NAME.')
    return encoded


class _RemoteFile(io.RawIOBase):
    """Seekable, ETag-pinned range reads for Parquet footers and previews."""
    def __init__(self, client, bucket, key, head):
        self.client, self.bucket, self.key = client, bucket, key
        self.size, self.etag, self.position = head['ContentLength'], head['ETag'], 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

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


def _file_size(size):
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if size < 1000 or unit == 'TB':
            return f'{size:.0f} B' if unit == 'B' else f'{size:.2f} {unit}'
        size /= 1000


def list_tables(*, folder=None, search=None, sort_by='created_at', ascending=False):
    """List names, rows, UTC update times to the minute and readable file sizes.

    Newest additions first. Use describe() for exact bytes and full metadata.
    """
    if sort_by == 'size':
        sort_by = 'size_bytes'
    if sort_by not in SUMMARY_COLUMNS:
        raise ValueError('Unknown sort column: ' + str(sort_by))
    prefix = PREFIX
    if folder is not None:
        if not isinstance(folder, str):
            raise ValueError('folder must be a table-name prefix')
        folder = folder.rstrip('/')
        _key(folder)
        prefix += folder + '/'
    client, bucket = _connection()
    records = []
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.parquet'):
                continue
            name = key[len(PREFIX):-len('.parquet')]
            if search is not None and search.casefold() not in name.casefold():
                continue
            records.append(_summary(name, _head(client, bucket, key)))
    result = pd.DataFrame(records, columns=SUMMARY_COLUMNS).sort_values(
        sort_by, ascending=ascending, kind='stable', ignore_index=True,
    )
    result['size'] = result['size_bytes'].map(_file_size)
    result['updated_at'] = result['updated_at'].map(
        lambda value: datetime.fromisoformat(value).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M'))
    return result[['table', 'rows', 'updated_at', 'size', 'updated_by']]


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
    return _download(client, bucket, key, head, destination, overwrite)


def _download(client, bucket, key, head, destination, overwrite=True):
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


def read_table(table, *, columns=None, refresh=False):
    """Load current data using a local cache; refresh=True forces a new download."""
    key = _key(table)
    client, bucket = _connection()
    return _cache.read(
        _cache.scope(client, bucket), table, lambda: _head(client, bucket, key),
        lambda path, head: _download(client, bucket, key, head, path), columns, refresh,
    )


def cache_info():
    """List cached tables, byte sizes and last-use times for the configured bucket."""
    client, bucket = _connection()
    return _cache.info(_cache.scope(client, bucket))


def clear_cache(table=None):
    """Remove local cached files for one table or this bucket. Returns the number removed."""
    if table is not None:
        _key(table)
    client, bucket = _connection()
    return _cache.clear(_cache.scope(client, bucket), table)


def _invalidate_cache(client, bucket, table):
    try:
        _cache.clear(_cache.scope(client, bucket), table)
    except Exception as exc:
        warnings.warn(
            f'R2 change succeeded for {table}, but local cache cleanup failed '
            f'({type(exc).__name__}). Use clear_cache() to retry cleanup.',
            RuntimeWarning, stacklevel=2,
        )


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
    if description is None:
        description = previous['description'] if previous else ''
    info = dict(
        created_at=previous['created_at'] if previous else datetime.now(timezone.utc).isoformat(),
        description=description,
        rows=rows, columns=columns, updated_by=(_settings().get('YOUR_NAME') or '').strip(),
    )
    if not isinstance(info['description'], str):
        raise TypeError('description must be a string')
    metadata = _metadata(info)
    # Data and metadata become visible together after the upload completes.
    client.upload_file(str(path), bucket, key, ExtraArgs={
        'Metadata': {'ubctgdb': metadata}, 'ContentType': 'application/vnd.apache.parquet',
    })
    _invalidate_cache(client, bucket, table)
    return _summary(table, _head(client, bucket, key))


def upload_dataframe(df, *, table, description=None, replace_table=False):
    """Publish a DataFrame as Zstandard-compressed Parquet, without its index."""
    key = _key(table)
    if not replace_table:
        client, bucket = _connection()
        try:
            _head(client, bucket, key)
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError('Table exists; pass replace_table=True to replace it.')
    with tempfile.TemporaryDirectory(prefix='ubctgdb-') as folder:
        path = Path(folder) / 'data.parquet'
        df.to_parquet(path, index=False, compression='zstd', row_group_size=10_000)
        return upload_parquet(path, table=table, description=description, replace_table=replace_table)


def delete_table(table):
    """Permanently delete a table. Missing names raise FileNotFoundError."""
    key = _key(table)
    client, bucket = _connection()
    _head(client, bucket, key)
    client.delete_object(Bucket=bucket, Key=key)
    _invalidate_cache(client, bucket, table)
    return {'table': table, 'deleted': True}


def _copy_table(client, bucket, source, destination, head, metadata):
    args = dict(Bucket=bucket, Key=destination, CopySource={'Bucket': bucket, 'Key': source})
    if head['ContentLength'] <= _COPY_LIMIT:
        client.copy_object(**args, CopySourceIfMatch=head['ETag'], MetadataDirective='REPLACE',
                           Metadata=metadata, ContentType=head.get('ContentType', 'application/vnd.apache.parquet'))
        return
    # R2 does not support conditional UploadPartCopy. Check the source again below.
    upload = client.create_multipart_upload(
        Bucket=bucket, Key=destination, Metadata=metadata,
        ContentType=head.get('ContentType', 'application/vnd.apache.parquet'),
    )
    target = dict(Bucket=bucket, Key=destination, UploadId=upload['UploadId'])
    try:
        parts = []
        part_size = max(_COPY_PART_SIZE, (head['ContentLength'] + 9999) // 10000)
        for number, start in enumerate(range(0, head['ContentLength'], part_size), 1):
            end = min(start + part_size, head['ContentLength']) - 1
            result = client.upload_part_copy(
                **args, UploadId=upload['UploadId'], PartNumber=number,
                CopySourceRange=f'bytes={start}-{end}',
            )
            parts.append({'PartNumber': number, 'ETag': result['CopyPartResult']['ETag']})
        client.complete_multipart_upload(**target, MultipartUpload={'Parts': parts})
    except Exception:
        try:
            client.abort_multipart_upload(**target)
        except Exception:
            pass
        raise


def rename_table(old_name, new_name):
    """Copy, verify size/metadata, then delete the old name. Coordinate one writer per name."""
    source, destination = _key(old_name), _key(new_name)
    if source == destination:
        raise ValueError('The new name must differ from the old name.')
    client, bucket = _connection()
    before = _head(client, bucket, source)
    try:
        _head(client, bucket, destination)
    except FileNotFoundError:
        pass
    else:
        raise FileExistsError('Destination table already exists: ' + new_name)
    metadata = dict(before.get('Metadata', {}))
    info = json.loads(metadata.get('ubctgdb', '{}'))
    info.setdefault('created_at', before['LastModified'].isoformat())
    info['updated_by'] = (_settings().get('YOUR_NAME') or '').strip()
    metadata['ubctgdb'] = _metadata(info)
    _copy_table(client, bucket, source, destination, before, metadata)
    after = _head(client, bucket, source)
    copied = _head(client, bucket, destination)
    if (before['ETag'] != after['ETag']
            or before['ContentLength'] != copied['ContentLength']
            or metadata != copied.get('Metadata', {})
            or before.get('Metadata', {}) != after.get('Metadata', {})):
        raise IOError('Rename verification failed; original retained. Inspect both table names before retrying.')
    try:
        client.delete_object(Bucket=bucket, Key=source)
    except Exception as exc:
        raise RuntimeError(
            f'Copied to {new_name}, but deletion of {old_name} was not confirmed. '
            'Both names may exist; check before retrying.'
        ) from exc
    for table in (old_name, new_name):
        _invalidate_cache(client, bucket, table)
    return {'old_name': old_name, 'new_name': new_name}
