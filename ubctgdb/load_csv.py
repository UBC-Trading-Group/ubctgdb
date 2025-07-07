from __future__ import annotations

import csv
import os
import re
import subprocess
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Mapping

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import sqlalchemy as sa
from dotenv import find_dotenv, load_dotenv

# ── .env handling ─────────────────────────────────────────────────────────────
dotenv = find_dotenv(usecwd=True)
if dotenv:
    load_dotenv(dotenv, override=False)
else:
    sys.stderr.write(
        "[load_csv] WARNING: .env not found; expecting DB_* vars in env.\n"
    )

DEFAULT_DB_PORT = int(os.getenv("DB_PORT", "3306"))

# ── INT size map (same as MySQL) ──────────────────────────────────────────────
_INT_LIMITS = [
    ("TINYINT",   -128,                     127,                    255),
    ("SMALLINT",  -32768,                   32767,                  65535),
    ("MEDIUMINT", -8388608,                 8388607,                16777215),
    ("INT",       -2147483648,              2147483647,             4294967295),
    ("BIGINT",    -9223372036854775808,     9223372036854775807,    18446744073709551615),
]

# ── Column stats container ────────────────────────────────────────────────────
class ColStats:
    __slots__ = (
        "max_len", "has_lz", "all_int", "all_num",
        "min_int", "max_int", "max_scale",
    )

    def __init__(self) -> None:
        self.max_len  = 0
        self.has_lz   = False
        self.all_int  = True   # only digits +/-?
        self.all_num  = True   # int or dec
        self.min_int  = 0
        self.max_int  = 0
        self.max_scale= 0      # decimal places

    # update with an Arrow STRING array (no nulls)
    def update(self, arr: pa.Array) -> None:
        if arr.null_count == len(arr):
            return

        # max length
        self.max_len = max(
            self.max_len,
            pc.max(pc.utf8_length(arr)).as_py()
        )

        # leading-zero flag
        lz_mask = pc.and_(
            pc.greater(pc.utf8_length(arr), 1),
            pc.equal(pc.substr(arr, 0, 1), pa.scalar("0")),
        )
        if pc.any(lz_mask).as_py():
            self.has_lz = True

        # digit / decimal regex masks
        int_mask = pc.match_substring_regex(arr, r"^[+-]?\d+$")
        dec_mask = pc.match_substring_regex(arr, r"^[+-]?\d+\.\d+$")

        if not pc.all(pc.or_(int_mask, lz_mask)).as_py():
            self.all_int = False
        if not pc.all(pc.or_(int_mask, dec_mask)).as_py():
            self.all_num = False

        if self.all_int:
            ints = pc.cast(arr, pa.int64(), safe=False)
            mn = pc.min(ints).as_py()
            mx = pc.max(ints).as_py()
            if self.min_int == self.max_int == 0:
                self.min_int, self.max_int = mn, mx
            else:
                self.min_int = min(self.min_int, mn)
                self.max_int = max(self.max_int, mx)

        if self.all_num and dec_mask.null_count != len(dec_mask):
            scale = pc.max(pc.utf8_length(pc.substr(arr, pc.index(arr, pc.find_substring(arr, ".")) + 1))).as_py()  # type: ignore
            self.max_scale = max(self.max_scale, scale)

# ── Arrow-based inference (streaming) ─────────────────────────────────────────
def _infer_column_types(path: Path, *, header: bool, delimiter: str=",") -> OrderedDict[str, str]:
    print("[infer] PyArrow streaming scan …")
    t0 = time.perf_counter()

    # figure out column names (needed for column_types mapping)
    with open(path, newline="", encoding="utf-8") as f:
        first_line = f.readline().rstrip("\n")

    if header:
        col_names = next(csv.reader([first_line]))
        skip_rows = 1
    else:
        col_count = len(next(csv.reader([first_line])))
        col_names = [f"col{i+1}" for i in range(col_count)]
        skip_rows = 0

    arrow_types = {name: pa.string() for name in col_names}

    reader = pacsv.open_csv(
        path,
        read_options=pacsv.ReadOptions(
            autogenerate_column_names=not header,
            skip_rows=skip_rows,
            block_size=1 << 20,          # 1 MiB batches ⇒ low RAM
        ),
        parse_options=pacsv.ParseOptions(delimiter=delimiter),
        convert_options=pacsv.ConvertOptions(
            column_types=arrow_types,
            null_values=["", "nan", "NaN", "NULL"],
        ),
    )

    stats = {name: ColStats() for name in col_names}
    total = 0
    for batch in reader:
        total += batch.num_rows
        for i, name in enumerate(col_names):
            col = batch.column(i).drop_null()
            if len(col):
                stats[name].update(col)

    print(f"[infer] Done – analysed {total:,} rows in {time.perf_counter()-t0:.1f}s")

    # map stats → MySQL types
    types: OrderedDict[str, str] = OrderedDict()
    for name, s in stats.items():
        if s.has_lz:
            types[name] = f"CHAR({s.max_len})"
            continue

        if s.all_int and s.max_len <= 20:
            unsigned = s.min_int >= 0
            for sql, lo, hi, uhi in _INT_LIMITS:
                if unsigned and s.max_int <= uhi:
                    types[name] = f"{sql} UNSIGNED"
                    break
                if not unsigned and lo <= s.min_int and s.max_int <= hi:
                    types[name] = sql
                    break
            else:
                types[name] = f"VARCHAR({s.max_len})"
            continue

        if s.all_num:
            prec = len(str(abs(s.max_int))) + s.max_scale
            types[name] = f"DECIMAL({prec},{s.max_scale})" if prec <= 38 else "DOUBLE"
            continue

        types[name] = "TEXT" if s.max_len > 255 else f"VARCHAR({s.max_len})"

    return types, col_names, skip_rows

# ── SQL helpers ───────────────────────────────────────────────────────────────
def _q(x: str) -> str:
    if "`" in x:
        raise ValueError("back-tick in identifier")
    return f"`{x}`"

def _eng(host: str, port: int) -> sa.engine.Engine:
    url = sa.engine.url.URL.create(
        "mysql+pymysql",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=host,
        port=port,
        database=None,
    )
    return sa.create_engine(url, pool_pre_ping=True, pool_recycle=1800)

def _create_table(host: str, port: int, schema: str, table: str,
                  col_types: Mapping[str, str]) -> None:
    cols_sql = ",\n  ".join(f"{_q(c)} {t}" for c, t in col_types.items())
    ddl = f"CREATE TABLE IF NOT EXISTS {_q(schema)}.{_q(table)} (\n  {cols_sql}\n) ENGINE=InnoDB;"
    with _eng(host, port).begin() as conn:
        conn.execute(sa.text(f"CREATE DATABASE IF NOT EXISTS {_q(schema)};"))
        conn.execute(sa.text(ddl))

def _mysqlsh_import(path: Path, *, host: str, port:int, schema:str,
                    table:str, cols:list[str], dialect:str,
                    threads:int, skip:int, dup:bool) -> None:
    user, pw = os.getenv("DB_USER"), os.getenv("DB_PASS")
    uri = f"mysql://{user}:{pw}@{host}:{port}"
    cmd = [
        "mysqlsh", uri, "--", "util", "import-table", str(path),
        f"--schema={schema}", f"--table={table}",
        f"--columns={','.join(cols)}",
        f"--dialect={dialect}", f"--threads={threads}",
        f"--skipRows={skip}",
        "--nullValue=''",          # tell mysqlsh to treat '' as NULL
        "--showProgress=true",
    ]
    if dup:
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
    Bulk-load *csv_path* into MySQL using **PyArrow-only** schema inference.

    Parameters
    ----------
    header : bool, default True
        True → first row contains column names.  
        False → file has no header; names generated col1…colN.
    """
    src = Path(csv_path).expanduser()
    if not src.exists():
        raise FileNotFoundError(src)

    schema = schema or os.getenv("DB_NAME")
    host   = host   or os.getenv("DB_HOST")
    port   = port   or DEFAULT_DB_PORT
    if not schema or not host:
        raise RuntimeError("DB_HOST and DB_NAME must be set (env or arg)")

    inferred_types, col_names, skip = _infer_column_types(src, header=header)
    if col_types:
        inferred_types.update(col_types)   # user override

    _create_table(host, port, schema, table, inferred_types)

    _mysqlsh_import(
        src, host=host, port=port,
        schema=schema, table=table,
        cols=list(inferred_types.keys()),
        dialect=dialect, threads=threads,
        skip=skip, dup=replace_duplicates,
    )

    print(f"[load_csv] Imported {src.name} → {schema}.{table} "
          f"({len(inferred_types)} columns)")
