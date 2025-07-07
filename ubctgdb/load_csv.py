from __future__ import annotations

import csv
import os
import re
import shutil
import subprocess
import tempfile
import time
from collections import OrderedDict
from pathlib import Path
from typing import Mapping

import sqlalchemy as sa
from dotenv import find_dotenv, load_dotenv

try:
    import pyarrow as pa
    import pyarrow.csv as pacsv
except ModuleNotFoundError as exc:
    raise ImportError("pyarrow >= 14 required.  pip install pyarrow") from exc


# ── env ------------------------------------------------------------------
dotenv_path = find_dotenv(usecwd=True)
if dotenv_path:
    load_dotenv(dotenv_path, override=False)

DEFAULT_DB_PORT = int(os.getenv("DB_PORT", "3306"))
_PROGRESS_EVERY = 500_000


# ── helpers --------------------------------------------------------------
def _q(x: str) -> str:
    if "`" in x:
        raise ValueError("back-tick in identifier")
    return f"`{x}`"


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


# ── clean ----------------------------------------------------------------
def _clean_inplace(src: Path) -> None:
    print(f"[clean] Overwriting {src.name} …")
    t0 = time.perf_counter()
    fd, tmp = tempfile.mkstemp(suffix=".csv", prefix="tmp_", dir=src.parent)
    os.close(fd)
    tmp_path = Path(tmp)

    with src.open(newline="", encoding="utf-8") as fin, \
         tmp_path.open("w", newline="", encoding="utf-8") as fout:
        r, w = csv.reader(fin), csv.writer(fout, lineterminator="\n")
        for i, row in enumerate(r, 1):
            w.writerow(
                ("\\N" if (s := cell.strip()) in {"", "nan", "NaN", "NULL"} else s)
                for cell in row
            )
            if i % _PROGRESS_EVERY == 0:
                print(f"[clean]   … {i:,} rows")
    os.replace(tmp_path, src)
    print(f"[clean] Done in {time.perf_counter() - t0:.1f} s")


# ── header detection -----------------------------------------------------
def _looks_like_data(s: str) -> bool:
    return (
        s.replace(".", "", 1).lstrip("-").isdigit()
        or (len(s) == 10 and s[4] == "-" and s[7] == "-")
    )


def _auto_header(path: Path) -> bool:
    with path.open("r", encoding="utf-8") as fh:
        first = next(csv.reader([fh.readline()]))
    auto = not any(_looks_like_data(x) for x in first)
    print(f"[load_csv] auto-detect header → {auto}")
    return auto


# ── Arrow schema ---------------------------------------------------------
def _infer_schema(path: Path, *, header: bool) -> OrderedDict[str, str]:
    print("[infer] PyArrow schema …")
    t0 = time.perf_counter()
    tbl = pacsv.read_csv(
        path,
        read_options=pacsv.ReadOptions(
            autogenerate_column_names=not header,
            skip_rows=1 if header else 0,
        ),
    )

    types: OrderedDict[str, str] = OrderedDict()
    for name, col in zip(tbl.schema.names, tbl.columns, strict=True):
        t = col.type
        if pa.types.is_boolean(t):
            types[name] = "TINYINT UNSIGNED"
        elif pa.types.is_integer(t):
            mysql = {8: "TINYINT", 16: "SMALLINT", 32: "INT", 64: "BIGINT"}[t.bit_width]
            types[name] = mysql if pa.types.is_signed_integer(t) else f"{mysql} UNSIGNED"
        elif pa.types.is_floating(t):
            types[name] = "DOUBLE"
        elif pa.types.is_decimal(t):
            p, s = t.precision, t.scale
            types[name] = f"DECIMAL({p},{s})" if p <= 38 else "DOUBLE"
        elif pa.types.is_date(t):
            types[name] = "DATE"
        elif pa.types.is_timestamp(t):
            types[name] = "DATETIME"
        elif pa.types.is_time(t):
            types[name] = "TIME"
        elif pa.types.is_string(t) or pa.types.is_binary(t):
            max_len = pa.compute.max(pa.compute.utf8_length(col)).as_py() or 0
            types[name] = "TEXT" if max_len > 255 else f"VARCHAR({max_len})"
        else:
            types[name] = "TEXT"
    print(f"[infer] Done in {time.perf_counter() - t0:.1f} s ({len(types)} cols)")
    return types


# ── safe column names ----------------------------------------------------
_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_names(d: OrderedDict[str, str]) -> OrderedDict[str, str]:
    out, used = OrderedDict(), set()
    for i, (n, t) in enumerate(d.items()):
        if not _ID_RE.match(n):
            n = f"col{i}"
        while n in used:
            i += 1
            n = f"col{i}"
        out[n] = t
        used.add(n)
    return out


# ── DDL ------------------------------------------------------------------
def _create_table(host: str, port: int, schema: str, table: str,
                  cols: Mapping[str, str], *, replace: bool) -> None:
    with _sqlalchemy_engine(host, port).begin() as conn:
        conn.execute(sa.text(f"CREATE DATABASE IF NOT EXISTS {_q(schema)}"))
        if replace:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {_q(schema)}.{_q(table)}"))
        ddl = ",\n  ".join(f"{_q(c)} {t}" for c, t in cols.items())
        conn.execute(sa.text(
            f"CREATE TABLE IF NOT EXISTS {_q(schema)}.{_q(table)} (\n  {ddl}\n) ENGINE=InnoDB;"
        ))


# ── mysqlsh import -------------------------------------------------------
def _mysqlsh(path: Path, *, host: str, port: int, schema: str, table: str,
             columns: list[str], dialect: str, threads: int,
             skip_rows: int, replace_dup: bool) -> None:
    if not shutil.which("mysqlsh"):
        raise RuntimeError("mysqlsh not found")
    uri = f"mysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{host}:{port}"
    cmd = [
        "mysqlsh", uri, "--", "util", "import-table", str(path),
        f"--schema={schema}", f"--table={table}",
        f"--columns={','.join(columns)}",
        f"--dialect={dialect}", f"--threads={threads}",
        f"--skipRows={skip_rows}", "--showProgress=true",
    ]
    if replace_dup:
        cmd.append("--onDuplicateKeyUpdate")
    subprocess.run(cmd, check=True)


# ── public API -----------------------------------------------------------
def load_csv(
    *,
    csv_path: str | Path,
    table: str,
    schema: str | None = None,
    host: str | None = None,
    port: int | None = None,
    header: bool | None = None,         # None → auto, True/False → explicit
    dialect: str = "csv-unix",
    threads: int = 8,
    replace_duplicates: bool = False,
    clean: bool = True,
    replace_table: bool = False,
) -> None:
    """
    Clean (optional) → infer schema → create table → mysqlsh import.

    header
        * True  – first row is column names (no sniffing)
        * False – file has no header, columns auto-named col0, col1, …
        * None  – auto-detect by sniffing first row
    """
    src = Path(csv_path).expanduser()
    if not src.exists():
        raise FileNotFoundError(src)

    schema = schema or os.getenv("DB_NAME")
    host   = host   or os.getenv("DB_HOST")
    port   = port   or DEFAULT_DB_PORT
    if not schema or not host:
        raise RuntimeError("DB_HOST and DB_NAME must be set")

    if clean:
        _clean_inplace(src)

    if header is None:
        header = _auto_header(src)

    types = _infer_schema(src, header=header)
    if not header:
        types = _safe_names(types)

    _create_table(host, port, schema, table, types, replace=replace_table)

    _mysqlsh(
        src, host=host, port=port, schema=schema, table=table,
        columns=list(types.keys()), dialect=dialect, threads=threads,
        skip_rows=1 if header else 0, replace_dup=replace_duplicates,
    )

    print(f"[load_csv] Imported {src.name} → {schema}.{table} "
          f"({len(types)} columns)")
