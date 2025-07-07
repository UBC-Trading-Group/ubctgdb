from __future__ import annotations

import csv
import os
import subprocess
import sys
import tempfile
import time
from collections import OrderedDict
from pathlib import Path
from typing import Mapping

import sqlalchemy as sa
from dotenv import find_dotenv, load_dotenv

try:
    import pyarrow.csv as pacsv
    import pyarrow as pa
except ModuleNotFoundError as exc:  # Arrow is mandatory
    raise ImportError(
        "pyarrow >= 14 is required for load_csv(). "
        "Install with:  pip install pyarrow"
    ) from exc


# ── environment ───────────────────────────────────────────────────────────────
dotenv_path = find_dotenv(usecwd=True)
if dotenv_path:
    load_dotenv(dotenv_path, override=False)

DEFAULT_DB_PORT = int(os.getenv("DB_PORT", "3306"))

# ── constants ────────────────────────────────────────────────────────────────
_PROGRESS_EVERY = 500_000  # rows between console prints during cleaning

_INT_LIMITS = [
    ("TINYINT",   -128,                     127,                    255),
    ("SMALLINT",  -32768,                   32767,                  65535),
    ("MEDIUMINT", -8388608,                 8388607,                16777215),
    ("INT",       -2147483648,              2147483647,             4294967295),
    ("BIGINT",    -9223372036854775808,     9223372036854775807,    18446744073709551615),
]


# ── helpers ───────────────────────────────────────────────────────────────────
def _q(ident: str) -> str:
    if "`" in ident:
        raise ValueError("back-tick in identifier")
    return f"`{ident}`"


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
    Replace *src* atomically by a version where '', NaN, NULL → \\N.
    """
    print(f"[clean] Overwriting {src.name} …")
    t0 = time.perf_counter()

    fd, tmp_name = tempfile.mkstemp(suffix=".csv", prefix="tmp_", dir=src.parent)
    os.close(fd)
    tmp_path = Path(tmp_name)

    with (
        open(src, newline="", encoding="utf-8") as fin,
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
                print(f"[clean]   … {i:,} rows rewritten")

    os.replace(tmp_path, src)
    print(f"[clean] Done in {time.perf_counter() - t0:.1f} s")


def _infer_with_arrow(
    path: Path,
    *,
    header: bool,
    delimiter: str = ",",
) -> OrderedDict[str, str]:
    """
    Use PyArrow to scan the cleaned CSV and map Arrow types → MySQL types.
    """
    print("[infer] PyArrow schema inference …")
    t0 = time.perf_counter()

    tbl = pacsv.read_csv(
        path,
        read_options=pacsv.ReadOptions(
            autogenerate_column_names=not header,
            skip_rows=1 if header else 0,
        ),
        parse_options=pacsv.ParseOptions(delimiter=delimiter),
    )

    col_types: OrderedDict[str, str] = OrderedDict()
    for name, col in zip(tbl.schema.names, tbl.columns, strict=True):
        pa_t = col.type

        if pa.types.is_boolean(pa_t):
            col_types[name] = "TINYINT UNSIGNED"

        elif pa.types.is_integer(pa_t):
            bits = pa_t.bit_width
            signed = pa_t.is_signed
            mysql = {8: "TINYINT", 16: "SMALLINT",
                     32: "INT", 64: "BIGINT"}[bits]
            col_types[name] = mysql if signed else f"{mysql} UNSIGNED"

        elif pa.types.is_floating(pa_t):
            col_types[name] = "DOUBLE"

        elif pa.types.is_decimal(pa_t):
            p = pa_t.precision
            s = pa_t.scale
            col_types[name] = f"DECIMAL({p},{s})" if p <= 38 else "DOUBLE"

        else:  # string, binary, etc.
            # Find max length quickly via compute kernel
            max_len = pa.compute.max(pa.compute.utf8_length(col)).as_py()
            col_types[name] = (
                "TEXT" if max_len > 255 else f"VARCHAR({max_len})"
            )

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
        f"CREATE TABLE IF NOT EXISTS {_q(schema)}.{_q(table)} (\n  "
        f"{ddl_cols}\n) ENGINE=InnoDB;"
    )
    with _sqlalchemy_engine(host, port).begin() as conn:
        conn.execute(sa.text(f"CREATE DATABASE IF NOT EXISTS {_q(schema)};"))
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
    uri = f"mysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{host}:{port}"
    cmd = [
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
        "--showProgress=true",  # built-in progress bar
    ]
    if replace_duplicates:
        cmd.append("--onDuplicateKeyUpdate")
    subprocess.run(cmd, check=True)


# ── public API ────────────────────────────────────────────────────────────────
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
) -> None:
    """
    Clean a large CSV **in-place**, infer column types with PyArrow, and bulk-load
    it into MySQL using MySQL Shell’s parallel importer.

    Parameters
    ----------
    csv_path : path-like
        The CSV file to import (will be overwritten with a cleaned copy ⚠).
    table : str
        Destination table name.
    header : bool, default True
        True  → first row holds column names.  
        False → no header; columns auto-named col1, col2, …
    replace_duplicates : bool
        If True, pass `--onDuplicateKeyUpdate` to mysqlsh.
    """
    src = Path(csv_path).expanduser()
    if not src.exists():
        raise FileNotFoundError(src)

    schema = schema or os.getenv("DB_NAME")
    host = host or os.getenv("DB_HOST")
    port = port or DEFAULT_DB_PORT
    if not schema or not host:
        raise RuntimeError("DB_HOST and DB_NAME must be set (env or argument)")

    # 1 · Clean (in-place)
    _clean_inplace(src)

    # 2 · Infer types (PyArrow)
    col_types = _infer_with_arrow(src, header=header)

    # 3 · Create table if missing
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
