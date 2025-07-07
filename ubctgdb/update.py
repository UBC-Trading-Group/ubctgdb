from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd
import sqlalchemy as sa

from .core import NULL_TOKEN, q as _q, sqlalchemy_engine as _eng
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
    Append/merge *csv_path* into *schema.table*, preventing duplicates.

    • **staging** (default): Inserts new rows based on a logical key.
      It finds all rows in the CSV whose values in `key_cols` do not
      exist in the target table and inserts only them. This is the most
      robust method for general-purpose updates.

    • **watermark**: Assumes a monotonically increasing column (e.g., a date).
      It finds the maximum value of the first column in `key_cols` in the
      target table and only inserts rows from the CSV that are newer.
      This is faster for simple time-series appends.
    """
    if mode not in {"staging", "watermark"}:
        raise ValueError("mode must be 'staging' or 'watermark'")

    schema = schema or os.getenv("DB_NAME")
    host   = host   or os.getenv("DB_HOST")
    port   = port   or int(os.getenv("DB_PORT", "3306"))
    if not schema or not host:
        raise RuntimeError("DB_HOST and DB_NAME must be set")

    if mode == "watermark":
        _append_watermark(csv_path, table, key_cols, schema, host, port, **upload_csv_kw)
    else:
        _append_staging(csv_path, table, key_cols, schema, host, port, **upload_csv_kw)


def _append_staging(csv_path, table, key_cols, schema, host, port, **upload_csv_kw):
    """
    Uploads CSV to a staging table, then inserts only rows with keys that
    don't already exist in the target table.
    """
    key_list = list(key_cols)
    if not key_list:
        raise ValueError("key_cols must be non-empty for 'staging' mode")

    stage = f"{table}_staging_{int(time.time())}"
    upload_csv(
        csv_path=csv_path, table=stage, schema=schema, host=host, port=port,
        replace_table=True, **upload_csv_kw
    )

    with _eng(database=schema, host=host, port=port).begin() as conn:
        insp = sa.inspect(conn)
        all_cols = [c["name"] for c in insp.get_columns(stage, schema=schema)]
        
        # CORRECTED: Prefix all columns with `s.` to resolve ambiguity.
        cols_to_insert = ", ".join(_q(c) for c in all_cols)
        cols_to_select = ", ".join(f"s.{_q(c)}" for c in all_cols)
        
        join_conditions = " AND ".join(
            f"t.{_q(k)} = s.{_q(k)}" for k in key_list
        )
        
        # This query finds all rows in the staging table `s` that do not have a
        # matching key in the target table `t` and inserts them.
        query = f"""
            INSERT INTO {_q(schema)}.{_q(table)} ({cols_to_insert})
            SELECT {cols_to_select}
            FROM {_q(schema)}.{_q(stage)} AS s
            LEFT JOIN {_q(schema)}.{_q(table)} AS t ON {join_conditions}
            WHERE t.{_q(key_list[0])} IS NULL
        """
        
        conn.execute(sa.text(query))
        conn.execute(sa.text(f"DROP TABLE {_q(schema)}.{_q(stage)}"))


def _append_watermark(csv_path, table, key_cols, schema, host, port, **upload_csv_kw):
    """
    Uploads CSV to a staging table, deletes rows from staging that are older
    than the max "watermark" column in the target table, then inserts the rest.
    """
    try:
        date_col = next(iter(key_cols))
    except StopIteration:
        raise ValueError("key_cols must be non-empty for 'watermark' mode")

    stage = f"{table}_staging_{int(time.time())}"
    upload_csv(
        csv_path=csv_path, table=stage, schema=schema, host=host, port=port,
        replace_table=True, **upload_csv_kw,
    )

    with _eng(database=schema, host=host, port=port).begin() as conn:
        max_date = conn.scalar(
            sa.text(f"SELECT MAX({_q(date_col)}) FROM {_q(schema)}.{_q(table)}")
        )

        # If a max_date exists, remove all rows from the staging table that
        # are not newer than it.
        if max_date is not None:
            conn.execute(
                sa.text(f"DELETE FROM {_q(schema)}.{_q(stage)} WHERE {_q(date_col)} <= :max_date"),
                {"max_date": max_date}
            )

        # Insert all remaining (i.e., new) rows from staging into the target.
        # Use INSERT IGNORE as a final safeguard against odd edge cases like
        # duplicate rows within the new CSV data itself.
        insp = sa.inspect(conn)
        all_cols = [c["name"] for c in insp.get_columns(stage, schema=schema)]
        cols_sql = ", ".join(_q(c) for c in all_cols)
        
        conn.execute(sa.text(
            f"INSERT IGNORE INTO {_q(schema)}.{_q(table)} ({cols_sql}) "
            f"SELECT {cols_sql} FROM {_q(schema)}.{_q(stage)}"
        ))
        
        conn.execute(sa.text(f"DROP TABLE {_q(schema)}.{_q(stage)}"))


def append_dataframe(df: pd.DataFrame, **kw) -> None:
    """Same API as :func:`append_csv`, but starts from a DataFrame."""
    with tempfile.NamedTemporaryFile(
        suffix=".csv", prefix="df_", delete=False, mode="w", encoding="utf-8"
    ) as tmp:
        path = Path(tmp.name)
        df.to_csv(path, index=False, na_rep=NULL_TOKEN)
        # We need to close the file handle before append_csv opens it
    
    try:
        append_csv(csv_path=path, **kw)
    finally:
        path.unlink(missing_ok=True)