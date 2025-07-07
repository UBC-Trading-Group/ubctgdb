from __future__ import annotations

import csv
import os
import shutil
import subprocess
import tempfile
import time
import pyarrow as pa
import pyarrow.csv as pacsv

from collections import OrderedDict
from pathlib import Path
from typing import Mapping

import sqlalchemy as sa
from dotenv import find_dotenv, load_dotenv


# ── environment ────────────────────────────────────────────────────────────
dotenv_path = find_dotenv(usecwd=True)
if dotenv_path:
    load_dotenv(dotenv_path, override=False)

DEFAULT_DB_PORT: int = int(os.getenv("DB_PORT", "3306"))
_PROGRESS_EVERY: int = 500_000             # always print every N lines


# ── helpers ────────────────────────────────────────────────────────────────
def _q(identifier: str) -> str:
    """Back-tick quote a MySQL identifier."""
    if "`" in identifier:
        raise ValueError("back-tick in identifier")
    return f"`{identifier}`"


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


def _clean_inplace(src: Path) -> None:
    """
    Atomically overwrite *src* so that '', NaN, NULL → \N
    (MySQL’s NULL sentinel for LOAD DATA / mysqlsh import-table).
    """
    print(f"[clean] Overwriting {src.name} …")
    t0 = time.perf_counter()

    fd, tmp_name = tempfile.mkstemp(suffix=".csv", prefix="tmp_", dir=src.parent)
    os.close(fd)
    tmp_path = Path(tmp_name)

    with src.open(newline="", encoding="utf-8") as fin, \
         tmp_path.open("w", newline="", encoding="utf-8") as fout:

        reader = csv.reader(fin)
        writer = csv.writer(fout, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

        for i, row in enumerate(reader, 1):
            writer.writerow(
                ("\N" if (cell := cell.strip()) in {"", "nan", "NaN", "NULL"} else cell)
                for cell in row
            )
            if i % _PROGRESS_EVERY == 0:                    # always prints
                print(f"[clean]   … {i:,} rows rewritten")

    os.replace(tmp_path, src)
    print(f"[clean] Done in {time.perf_counter() - t0:.1f} s")


def _infer_with_arrow(path: Path, *, header: bool) -> OrderedDict[str, str]:
    """
    Scan the (cleaned or original) CSV with PyArrow and map Arrow → MySQL types.
    """
    print("[infer] PyArrow schema inference …")
    t0 = time.perf_counter()

    tbl = pacsv.read_csv(
        path,
        read_options=pacsv.ReadOptions(
            autogenerate_column_names=not header,
            skip_rows=1 if header else 0,
        ),
        # default delimiter “,” – no ParseOptions needed
    )

    col_types: OrderedDict[str, str] = OrderedDict()
    for name, col in zip(tbl.schema.names, tbl.columns, strict=True):
        pa_t = col.type

        if pa.types.is_boolean(pa_t):
            col_types[name] = "TINYINT UNSIGNED"

        elif pa.types.is_integer(pa_t):
            mysql = {8: "TINYINT", 16: "SMALLINT", 32: "INT", 64: "BIGINT"}[pa_t.bit_width]
            signed = pa.types.is_signed_integer(pa_t)         # robust signedness check
            col_types[name] = mysql if signed else f"{mysql} UNSIGNED"

        elif pa.types.is_floating(pa_t):
            col_types[name] = "DOUBLE"

        elif pa.types.is_decimal(pa_t):
            p, s = pa_t.precision, pa_t.scale
            col_types[name] = f"DECIMAL({p},{s})" if p <= 38 else "DOUBLE"

        else:  # string, binary, etc.
            max_len = pa.compute.max(pa.compute.utf8_length(col)).as_py()
            col_types[name] = "TEXT" if max_len > 255 else f"VARCHAR({max_len})"

    print(f"[infer] Finished in {time.perf_counter() - t0:.1f} s "
          f"({len(col_types)} columns)")
    return col_types


def _create_table(
    *,
    host: str,
    port: int,
    schema: str,
    table: str,
    col_types: Mapping[str, str],
) -> None:
    ddl_cols = ",\n  ".join(f"{_q(c)} {t}" for c, t in col_types.items())
    ddl = (
        f"CREATE DATABASE IF NOT EXISTS {_q(schema)};\n"
        f"CREATE TABLE IF NOT EXISTS {_q(schema)}.{_q(table)} (\n  "
        f"{ddl_cols}\n) ENGINE=InnoDB;"
    )
    with _sqlalchemy_engine(host, port).begin() as conn:
        conn.execute(sa.text(ddl))


def _mysqlsh_import(
    path: Path,
    *,
    host: str,
    port: int,
    schema: str,
    table: str,
    columns: list[str],
    dialect: str,
    threads: int,
    skip_rows: int,
    replace_duplicates: bool,
) -> None:
    if not shutil.which("mysqlsh"):
        raise RuntimeError("mysqlsh not found; install MySQL Shell ≥ 8.0.22")

    uri = f"mysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{host}:{port}"
    cmd: list[str] = [
        "mysqlsh",
        uri,
        "--",
        "util",
        "import-table",
        str(path),
        f"--schema={schema}",
        f"--table={table}",
        f"--columns={','.join(columns)}",
        f"--dialect={dialect}",
        f"--threads={threads}",
        f"--skipRows={skip_rows}",
        "--showProgress=true",               # built-in progress bar (always on)
    ]
    if replace_duplicates:
        cmd.append("--onDuplicateKeyUpdate")

    subprocess.run(cmd, check=True)


# ── public API ─────────────────────────────────────────────────────────────
def load_csv(
    *,
    csv_path: str | Path,
    table: str,
    schema: str | None = None,
    host: str | None = None,
    port: int | None = None,
    header: bool = True,
    dialect: str = "csv-unix",
    threads: int = 8,
    replace_duplicates: bool = False,
    clean: bool = True,                     # ← NEW FLAG
) -> None:
    """
    Import a (large) CSV into MySQL via MySQL-Shell’s parallel importer.

    Steps:
      1. **Optionally** clean the CSV in-place (''/NaN/NULL → \N).
      2. Infer column types with PyArrow.
      3. Create the destination table if needed.
      4. Bulk-load with `mysqlsh util import-table`.

    Parameters
    ----------
    csv_path : str | Path
        Path to the CSV file (will be overwritten if `clean=True`).
    clean : bool, default True
        If False, skip step 1 (useful if the file is already in MySQL-friendly
        form or you need maximum speed).
    """
    src = Path(csv_path).expanduser()
    if not src.exists():
        raise FileNotFoundError(src)

    schema = schema or os.getenv("DB_NAME")
    host   = host   or os.getenv("DB_HOST")
    port   = port   or DEFAULT_DB_PORT
    if not schema or not host:
        raise RuntimeError("DB_HOST and DB_NAME must be set (env or argument)")

    # 1 · Clean (optional)
    if clean:
        _clean_inplace(src)

    # 2 · Infer types
    col_types = _infer_with_arrow(src, header=header)

    # 3 · Create table (if absent)
    _create_table(
        host=host,
        port=port,
        schema=schema,
        table=table,
        col_types=col_types,
    )

    # 4 · Import via mysqlsh
    _mysqlsh_import(
        src,
        host=host,
        port=port,
        schema=schema,
        table=table,
        columns=list(col_types.keys()),
        dialect=dialect,
        threads=threads,
        skip_rows=1 if header else 0,
        replace_duplicates=replace_duplicates,
    )

    print(
        f"[load_csv] Imported {src.name} → {schema}.{table} "
        f"({len(col_types)} columns)"
    )
