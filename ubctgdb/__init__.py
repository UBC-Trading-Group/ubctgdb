"""Simple shared Parquet tables on Cloudflare R2."""
from .core import (
    list_tables, describe, preview, read_table, download_table,
    upload_dataframe, upload_parquet, delete_table, rename_table,
)

__version__ = '1.1.0'
__all__ = [
    'list_tables', 'describe', 'preview', 'read_table', 'download_table',
    'upload_dataframe', 'upload_parquet', 'delete_table', 'rename_table',
]
