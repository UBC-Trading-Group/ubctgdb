from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd
import sqlalchemy as sa

from .core import q as _q, sqlalchemy_engine as _eng
from .upload_csv import upload_csv

# ── simple “staging table” strategy ──────────────────────────────────────
def append_csv(
    *,
    csv_path: str | Path,
    table: str,
    key_cols: Iterable[str],
    schema: str | None = None,
    host: str | None = None,
    port: int | None = None,
    mode: Literal["staging", "watermark"] = "staging",
    **upload_csv_kw,
) -> None:
    """
    Append/merge *csv_path* into *schema.table*.

    • **staging** (default): bulk-load into temp table → INSERT IGNORE.
    • **watermark**: assumes a monotone ‘date’ column; skips older rows.
    """
    if mode not in {"staging", "watermark"}:
        raise ValueError("mode must be 'staging' or 'watermark'")

    schema = schema or os.getenv("DB_NAME")
    host   = host   or os.getenv("DB_HOST")
    port   = port   or int(os.getenv("DB_PORT", "3306"))

    if mode == "watermark":
        _append_watermark(csv_path, table, key_cols, schema, host, port, upload_csv_kw)
    else:
        _append_staging(csv_path, table, key_cols, schema, host, port, upload_csv_kw)

def _append_staging(csv_path, table, key_cols, schema, host, port, upload_csv_kw):
    stage = f"{table}_staging_tmp"
    upload_csv(
        csv_path=csv_path,
        table=stage,
        schema=schema,
        host=host,
        port=port,
        replace_table=True,
        **upload_csv_kw,
    )
    keys = ", ".join(_q(k) for k in key_cols)
    cols = ", ".join(_q(c) for c in key_cols)  # all columns assumed same
    with _eng(host=host, port=port).begin() as conn:
        conn.execute(sa.text(
            f"INSERT IGNORE INTO {_q(schema)}.{_q(table)} "
            f"SELECT * FROM {_q(schema)}.{_q(stage)}"
        ))
        conn.execute(sa.text(f"DROP TABLE {_q(schema)}.{_q(stage)}"))

def _append_watermark(csv_path, table, key_cols, schema, host, port, upload_csv_kw):
    date_col = next(iter(key_cols))
    with _eng(host=host, port=port).begin() as conn:
        max_date = conn.scalar(
            sa.text(f"SELECT MAX({_q(date_col)}) FROM {_q(schema)}.{_q(table)}")
        ) or "1900-01-01"
    upload_csv(
        csv_path=csv_path,
        table=table,
        schema=schema,
        host=host,
        port=port,
        replace_table=False,
        upload_csv_kw=upload_csv_kw,
        replace_duplicates=True,
        header=None,
        clean=True,
    )

def append_dataframe(df: pd.DataFrame, **kw) -> None:
    """Same API as :func:`append_csv`, but starts from a DataFrame."""
    with tempfile.NamedTemporaryFile(suffix=".csv", prefix="df_") as tmp:
        path = Path(tmp.name)
        df.to_csv(path, index=False)
        append_csv(csv_path=path, **kw)
