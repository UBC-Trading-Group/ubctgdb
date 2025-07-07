from __future__ import annotations

import csv
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import OrderedDict
from functools import reduce
from pathlib import Path
from typing import Mapping

import sqlalchemy as sa
from concurrent.futures import ProcessPoolExecutor, as_completed
from dotenv import find_dotenv, load_dotenv

# ── Environment ───────────────────────────────────────────────────────────────
dotenv_path = find_dotenv(usecwd=True)
if dotenv_path:
    load_dotenv(dotenv_path, override=False)
else:
    sys.stderr.write(
        "[load_csv] WARNING: .env not found; expecting DB_* vars in environment.\n"
    )

DEFAULT_DB_PORT = int(os.getenv("DB_PORT", "3306"))

# ── Progress logging config ───────────────────────────────────────────────────
_PROGRESS_EVERY: int | None = 500_000          # rows between prints
_CHUNK_SIZE = 250_000                          # rows per worker chunk

# ── CSV NULL-cleaner (streaming) ──────────────────────────────────────────────
def _clean_csv_to_temp(src_path: Path) -> Path:
    """Stream src_path → temp file, replacing '', NaN, NULL with \\N."""
    print(f"[clean] Streaming {src_path.name} → NULL-safe temp file …")
    t0 = time.perf_counter()

    fd, tmp_name = tempfile.mkstemp(
        suffix=".csv", prefix="clean_", dir=src_path.parent
    )
    os.close(fd)
    tmp_path = Path(tmp_name)

    with (
        open(src_path, newline="", encoding="utf-8") as fin,
        open(tmp_path, "w", newline="", encoding="utf-8") as fout,
    ):
        reader = csv.reader(fin)
        writer = csv.writer(fout, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

        for i, row in enumerate(reader, 1):
            writer.writerow(
                [
                    r"\N" if cell.strip() in {"", "nan", "NaN", "NULL"} else cell
                    for cell in row
                ]
            )
            if _PROGRESS_EVERY and i % _PROGRESS_EVERY == 0:
                print(f"[clean]   … {i:,} rows processed")

    print(f"[clean] Done in {time.perf_counter() - t0:.1f} s. Temp file → {tmp_path}")
    return tmp_path

# ── Datatype inference helpers ────────────────────────────────────────────────
_INT_LIMITS = [
    ("TINYINT",   -128,                     127,                    255),
    ("SMALLINT",  -32768,                   32767,                  65535),
    ("MEDIUMINT", -8388608,                 8388607,                16777215),
    ("INT",       -2147483648,              2147483647,             4294967295),
    ("BIGINT",    -9223372036854775808,     9223372036854775807,    18446744073709551615),
]

_META_TEMPLATE = {
    "max_len": 0,
    "has_lz": False,
    "is_int": True,
    "is_dec": True,
    "min_int": 0,
    "max_int": 0,
    "max_scale": 0,
}

def _feed_cell(cell: str, m: dict) -> None:
    if cell in {"", r"\N"}:
        return
    m["max_len"] = max(m["max_len"], len(cell))
    if len(cell) > 1 and cell[0] == "0":
        m["has_lz"] = True

    if m["is_int"] or m["is_dec"]:
        if cell.isdigit():
            val = int(cell)
            if m["min_int"] == m["max_int"] == 0:
                m["min_int"] = m["max_int"] = val
            else:
                m["min_int"] = min(m["min_int"], val)
                m["max_int"] = max(m["max_int"], val)
        else:
            if "." in cell:
                left, right = cell.split(".", 1)
                if left.lstrip("+-").isdigit() and right.isdigit():
                    m["is_int"] = False
                    m["max_scale"] = max(m["max_scale"], len(right))
                    val_int = int(left or "0")
                    m["min_int"] = min(m["min_int"], val_int)
                    m["max_int"] = max(m["max_int"], val_int)
                else:
                    m["is_int"] = m["is_dec"] = False
            else:
                m["is_int"] = m["is_dec"] = False

def _chunk_stats(rows: list[list[str]], ncols: int) -> list[dict]:
    """Compute meta for one chunk."""
    meta = [{**_META_TEMPLATE} for _ in range(ncols)]
    for row in rows:
        for idx, cell in enumerate(row):
            _feed_cell(cell, meta[idx])
    return meta

def _merge_meta(a: list[dict], b: list[dict]) -> list[dict]:
    """Combine two meta lists."""
    out = []
    for ma, mb in zip(a, b):
        mc = ma.copy()
        mc["max_len"]  = max(ma["max_len"],  mb["max_len"])
        mc["has_lz"]   = ma["has_lz"] or mb["has_lz"]
        mc["is_int"]   = ma["is_int"] and mb["is_int"]
        mc["is_dec"]   = ma["is_dec"] and mb["is_dec"]
        mc["min_int"]  = min(ma["min_int"], mb["min_int"])
        mc["max_int"]  = max(ma["max_int"], mb["max_int"])
        mc["max_scale"]= max(ma["max_scale"], mb["max_scale"])
        out.append(mc)
    return out

def _infer_column_types(csv_path: Path, *, header: bool, delim: str=",") -> OrderedDict[str, str]:
    """Multi-process streaming scan → OrderedDict{name: mysql_type}."""
    print(f"[infer] Scanning {csv_path.name} (multi-process) …")
    t0 = time.perf_counter()

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=delim)

        # Determine column names
        if header:
            header_row = next(reader)
            col_names = header_row
        else:
            first_row = next(reader)
            col_names = [f"col{i+1}" for i in range(len(first_row))]

        ncols = len(col_names)

        # Dispatch chunks to workers
        futures = []
        rows: list[list[str]] = []        # current chunk buffer
        total_rows = 0

        with ProcessPoolExecutor() as pool:
            def _dispatch():
                if rows:
                    futures.append(pool.submit(_chunk_stats, rows.copy(), ncols))
                    rows.clear()

            if not header:
                rows.append(first_row)
                total_rows += 1

            for row in reader:
                rows.append(row)
                total_rows += 1
                if len(rows) >= _CHUNK_SIZE:
                    _dispatch()
                if _PROGRESS_EVERY and total_rows % _PROGRESS_EVERY == 0:
                    print(f"[infer]   … {total_rows:,} rows queued")

            _dispatch()  # last partial chunk

            # Reduce results
            meta_iter = (f.result() for f in as_completed(futures))
            meta = reduce(_merge_meta, meta_iter)

    print(f"[infer] Finished. Analysed {total_rows:,} rows in {time.perf_counter() - t0:.1f} s")

    # Map meta → MySQL types
    col_types: OrderedDict[str, str] = OrderedDict()
    for name, m in zip(col_names, meta):
        max_len = m["max_len"]

        if m["has_lz"]:
            col_types[name] = f"CHAR({max_len})"
            continue

        if m["is_int"] and max_len <= 20:
            unsigned = m["min_int"] >= 0
            chosen: str | None = None
            for sql, lo, hi, uhi in _INT_LIMITS:
                if unsigned and m["max_int"] <= uhi:
                    chosen = f"{sql} UNSIGNED"
                    break
                if not unsigned and lo <= m["min_int"] and m["max_int"] <= hi:
                    chosen = sql
                    break
            if chosen:
                col_types[name] = chosen
                continue

        if m["is_dec"]:
            precision = len(str(m["max_int"])) + m["max_scale"]
            if precision <= 38:
                col_types[name] = f"DECIMAL({precision},{m['max_scale']})"
            else:
                col_types[name] = "DOUBLE"
            continue

        col_types[name] = "TEXT" if max_len > 255 else f"VARCHAR({max_len})"

    return col_types

# ── SQL helpers ───────────────────────────────────────────────────────────────
def _sqlalchemy_engine(host: str, port: int) -> sa.engine.Engine:
    url = sa.engine.url.URL.create(
        drivername="mysql+pymysql",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=host,
        port=port,
        database=None,
    )
    return sa.create_engine(url, pool_pre_ping=True, pool_recycle=1800)

def _q(name: str) -> str:
    if "`" in name:
        raise ValueError(f"Back-tick in identifier: {name}")
    return f"`{name}`"

def _create_table(*, host: str, port: int, schema: str, table: str,
                  col_types: Mapping[str, str]) -> None:
    cols_sql = ",\n  ".join(f"{_q(c)} {t}" for c, t in col_types.items())
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {_q(schema)}.{_q(table)} (\n  "
        f"{cols_sql}\n) ENGINE=InnoDB;"
    )
    with _sqlalchemy_engine(host, port).begin() as conn:
        conn.execute(sa.text(f"CREATE DATABASE IF NOT EXISTS {_q(schema)};"))
        conn.execute(sa.text(ddl))

def _run_mysqlsh_import(*, csv_path: Path, host: str, port: int, schema: str,
                        table: str, columns: list[str], dialect: str, threads: int,
                        skip_rows: int, replace_duplicates: bool) -> None:
    user = os.getenv("DB_USER")
    pw   = os.getenv("DB_PASS")
    uri  = f"mysql://{user}:{pw}@{host}:{port}"

    cmd = [
        "mysqlsh", uri, "--", "util", "import-table", str(csv_path),
        f"--schema={schema}", f"--table={table}",
        f"--columns={','.join(columns)}",
        f"--dialect={dialect}",
        f"--threads={threads}",
        f"--skipRows={skip_rows}",
        "--showProgress=true",                 # built-in progress bar ✅
    ]
    if replace_duplicates:
        cmd.append("--onDuplicateKeyUpdate")

    subprocess.run(cmd, check=True)

# ── Public API ────────────────────────────────────────────────────────────────
def load_csv(
    *,
    csv_path: str | Path,
    table: str,
    schema: str | None = None,
    host: str | None = None,
    port: int | None = None,
    col_types: Mapping[str, str] | None = None,
    header: bool = True,
    dialect: str = "csv-unix",
    threads: int = 8,
    replace_duplicates: bool = False,
) -> None:
    """
    Bulk-load *csv_path* into *schema.table*.

    Parameters
    ----------
    header : bool, default True
        True  → first row is column names (skipped on import).  
        False → file has no header; columns auto-named col1…colN.
    All other parameters behave as in previous versions.
    """
    csv_path = Path(csv_path).expanduser()
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    schema = schema or os.getenv("DB_NAME")
    host   = host   or os.getenv("DB_HOST")
    port   = port   or DEFAULT_DB_PORT
    if schema is None or host is None:
        raise RuntimeError("DB_HOST and DB_NAME must be set (env or argument)")

    # 1. Clean NULL-likes
    cleaned_path = _clean_csv_to_temp(csv_path)

    # 2. Column-type inference (multi-process)
    if col_types is None:
        col_types = _infer_column_types(csv_path, header=header)

    # 3. Create table if not exists
    _create_table(
        host=host, port=port,
        schema=schema, table=table,
        col_types=col_types,
    )

    # 4. Import with MySQL Shell (built-in progress bar)
    skip_rows = 1 if header else 0
    _run_mysqlsh_import(
        csv_path=cleaned_path,
        host=host, port=port,
        schema=schema, table=table,
        columns=list(col_types.keys()),
        dialect=dialect,
        threads=threads,
        skip_rows=skip_rows,
        replace_duplicates=replace_duplicates,
    )

    print(
        f"[load_csv] Imported {csv_path.name} → {schema}.{table} "
        f"({len(col_types)} columns)"
    )
