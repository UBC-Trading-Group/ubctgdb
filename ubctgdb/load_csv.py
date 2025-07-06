from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import typing as t
from contextlib import contextmanager

import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Optional – these modules **only** exist inside the MySQL Shell interpreter.
# ---------------------------------------------------------------------------
try:
    import mysqlsh  # type: ignore
    from mysqlsh import util, mysql  # type: ignore

    IN_SHELL = True
except ModuleNotFoundError:  # running under CPython, not mysqlsh
    IN_SHELL = False

# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = "3306"
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER") 
DB_PASS = os.getenv("DB_PASS")


# Fallback if you do not already have a shared _engine() helper -------------

def _build_uri() -> str:  # mysqlsh style (mysqlx:// is implied)
    return f"{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _null_decode(cols: list[str]) -> dict[str, str]:
    """Return decodeColumns mapping that turns blanks → NULL."""
    return {c: f"NULLIF(@{c}, '')" for c in cols}


def _infer_types(df: pd.DataFrame) -> dict[str, str]:
    """Very light‑weight type inference – tweak as needed."""
    out: dict[str, str] = {}
    for c in df.columns:
        s = df[c]
        if pd.api.types.is_integer_dtype(s):
            out[c] = "BIGINT"
        elif pd.api.types.is_float_dtype(s):
            out[c] = "DECIMAL(38,18)"
        elif pd.api.types.is_datetime64_any_dtype(s):
            out[c] = "DATE"
        else:
            max_len = int(s.astype(str).str.len().max())
            out[c] = f"VARCHAR({max(32, min(max_len, 255))})"
    return out


def _render_create_sql(
    schema: str, table: str, col_types: dict[str, str], pk: list[str] | None
) -> str:
    cols = ",\n  ".join(f"`{c}` {t}" for c, t in col_types.items())
    pk_clause = f",\n  PRIMARY KEY ({', '.join(pk)})" if pk else ""
    return (
        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` (\n  {cols}{pk_clause}\n) "
        "ENGINE=InnoDB;"
    )


def _spawn_mysqlsh(uri: str, csv_path: str, opts: dict[str, t.Any]):
    """Run util.importTable in an external mysqlsh process when not embedded."""
    shell_code = (
        "import json, os, mysqlsh, util, mysql;"
        "csv=json.loads(os.environ['CSV']);"
        "opts=json.loads(os.environ['OPTS']);"
        "sess=mysql.get_session(os.environ['URI']);"
        "util.import_table(csv, opts)"
    )

    env = os.environ.copy() | {
        "URI": uri,
        "CSV": json.dumps(csv_path),
        "OPTS": json.dumps(opts),
    }

    cmd = ["mysqlsh", uri, "--py", "-e", shell_code]
    print("→ Spawning:", " ".join(cmd))
    subprocess.run(cmd, env=env, check=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_csv(
    csv_path: str,
    table: str,
    schema: str | None = None,
    *,
    col_types: dict[str, str] | None = None,
    primary_keys: list[str] | None = None,
    duplicate_mode: str = "ignore",  # "ignore" | "replace"
    skip_rows: int = 1,
    threads: int = 4,
    chunk_size: int = 100_000,
    show_progress: bool = True,
    infer_rows: int = 20_000,
    retries: int = 3,
):
    """Top‑level helper – call this from anywhere in your library."""

    schema = schema or DB_NAME

    # ---------------------------------------------------------------------
    # 1. Schema handling – either use the caller‑supplied mapping *or*
    #    infer from the first `infer_rows` rows.
    # ---------------------------------------------------------------------
    print(f"→ Loading {csv_path!r} → {schema}.{table} …")
    df_head = pd.read_csv(csv_path, nrows=infer_rows)
    columns = list(df_head.columns)

    if col_types is None:
        print(f"  Scanning first {infer_rows:,} rows to infer column types …")
        col_types = _infer_types(df_head)
        for c, t_ in col_types.items():
            print(f"    `{c}` {t_}")

    # ---------------------------------------------------------------------
    # 2. Build CREATE TABLE …
    # ---------------------------------------------------------------------
    create_sql = _render_create_sql(schema, table, col_types, primary_keys)

    # ---------------------------------------------------------------------
    # 3. Build util.importTable() options
    # ---------------------------------------------------------------------
    import_opts: dict[str, t.Any] = {
        "schema": schema,
        "table": table,
        "dialect": "csv-unix",
        "skipRows": skip_rows,
        "columns": columns,
        "decodeColumns": _null_decode(columns),
        "threads": threads,
        "rowsPerChunk": chunk_size,
        "onDuplicateKey": duplicate_mode,
        "showProgress": show_progress,
    }

    # ---------------------------------------------------------------------
    # 4. Either run embedded or spawn mysqlsh …
    # ---------------------------------------------------------------------
    uri = _build_uri()

    if IN_SHELL:
        sess = mysql.get_session(uri)
        sess.sql("SET SESSION net_write_timeout = 1800").execute()
        sess.sql("SET SESSION net_read_timeout  = 1800").execute()
        print(f"→ Connected to {uri.split('@')[1]} …")
        sess.sql(create_sql).execute()

        for attempt in range(1, retries + 1):
            try:
                util.import_table(csv_path, import_opts)
                print("✅  Import finished")
                break
            except Exception as exc:
                print(f"⚠️  attempt {attempt}/{retries} failed: {exc}")
                if attempt == retries:
                    raise
                back = 2 ** attempt
                print(f"   …retrying in {back}s")
                time.sleep(back)
    else:
        _spawn_mysqlsh(uri, csv_path, import_opts)


# ---------------------------------------------------------------------------
# CLI entry‑point (so you can:  python -m load_csv --csv …)
# ---------------------------------------------------------------------------

def _cli():
    ap = argparse.ArgumentParser(description="Fast parallel CSV → MySQL loader")
    ap.add_argument("--csv", required=True, help="Path to CSV file")
    ap.add_argument("--table", required=True, help="Target table name")
    ap.add_argument("--schema", default=None, help="Target schema (defaults to DB_NAME)")
    ap.add_argument("--threads", type=int, default=4, help="Parallel threads")
    args = ap.parse_args()

    load_csv(
        csv_path=args.csv,
        table=args.table,
        schema=args.schema,
        threads=args.threads,
    )


if __name__ == "__main__":
    _cli()
