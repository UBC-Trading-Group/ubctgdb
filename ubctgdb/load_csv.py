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
    raise ImportError(
        "pyarrow >= 14 is required for load_csv().  pip install pyarrow"
    ) from exc


# ── environment ---------------------------------------------------------------
dotenv_path = find_dotenv(usecwd=True)
if dotenv_path:
    load_dotenv(dotenv_path, override=False)

DEFAULT_DB_PORT = int(os.getenv("DB_PORT", "3306"))
_PROGRESS_EVERY = 500_000                       # console progress granularity


# ── misc helpers --------------------------------------------------------------
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


# ── cleaning ------------------------------------------------------------------
def _clean_inplace(src: Path) -> None:
    """''/NaN/NULL → \\N so MySQL reads them as NULL."""
    print(f"[clean] Overwriting {src.name} …")
    t0 = time.perf_counter()

    fd, tmp_name = tempfile.mkstemp(prefix="tmp_", suffix=".csv", dir=src.parent)
    os.close(fd)
    tmp_path = Path(tmp_name)

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


# ── header sniff --------------------------------------------------------------
def _looks_like_data(s: str) -> bool:
    return (
        s.replace(".", "", 1).lstrip("-").isdigit() or
        (len(s) == 10 and s[4] == "-" and s[7] == "-")
    )


def _detect_header(path: Path, assume_header: bool) -> bool:
    """Flip to False if the first row looks numeric/date-like."""
    if not assume_header:
        return False
    with path.open("r", encoding="utf-8") as fh:
        first = next(csv.reader([fh.readline()]))
    if any(_looks_like_data(x) for x in first):
        print("[load_csv] First row looks like data → header=False")
        return False
    return True


# ── Arrow schema --------------------------------------------------------------
def _infer_with_arrow(path: Path, *, header: bool) -> OrderedDict[str, str]:
    print("[infer] PyArrow schema inference …")
    t0 = time.perf_counter()

    tbl = pacsv.read_csv(
        path,
        read_options=pacsv.ReadOptions(
            autogenerate_column_names=not header,
            skip_rows=1 if header else 0,
        ),
    )

    col_types: OrderedDict[str, str] = OrderedDict()
    for name, col in zip(tbl.schema.names, tbl.columns, strict=True):
        t = col.type
        if pa.types.is_boolean(t):
            col_types[name] = "TINYINT UNSIGNED"
        elif pa.types.is_integer(t):
            mysql = {8: "TINYINT", 16: "SMALLINT", 32: "INT", 64: "BIGINT"}[t.bit_width]
            col_types[name] = mysql if pa.types.is_signed_integer(t) else f"{mysql} UNSIGNED"
        elif pa.types.is_floating(t):
            col_types[name] = "DOUBLE"
        elif pa.types.is_decimal(t):
            p, s = t.precision, t.scale
            col_types[name] = f"DECIMAL({p},{s})" if p <= 38 else "DOUBLE"
        elif pa.types.is_date(t):
            col_types[name] = "DATE"
        elif pa.types.is_timestamp(t):
            col_types[name] = "DATETIME"
        elif pa.types.is_time(t):
            col_types[name] = "TIME"
        elif pa.types.is_string(t) or pa.types.is_binary(t):
            max_len = pa.compute.max(pa.compute.utf8_length(col)).as_py() or 0
            col_types[name] = "TEXT" if max_len > 255 else f"VARCHAR({max_len})"
        else:
            col_types[name] = "TEXT"

    print(f"[infer] Finished in {time.perf_counter() - t0:.1f} s "
          f"({len(col_types)} columns)")
    return col_types


# ── make legal column names ---------------------------------------------------
_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_names(col_types: OrderedDict[str, str]) -> OrderedDict[str, str]:
    out: OrderedDict[str, str] = OrderedDict()
    used: set[str] = set()
    for i, (name, typ) in enumerate(col_types.items()):
        if not _ID_RE.match(name):
            name = f"col{i}"
        while name in used:
            i += 1
            name = f"col{i}"
        out[name] = typ
        used.add(name)
    return out


# ── DDL -----------------------------------------------------------------------
def _create_table(*, host: str, port: int,
                  schema: str, table: str,
                  col_types: Mapping[str, str]) -> None:
    cols = ",\n  ".join(f"{_q(c)} {t}" for c, t in col_types.items())
    ddl = f"CREATE TABLE IF NOT EXISTS {_q(schema)}.{_q(table)} (\n  {cols}\n) ENGINE=InnoDB;"
    with _sqlalchemy_engine(host, port).begin() as conn:
        conn.execute(sa.text(f"CREATE DATABASE IF NOT EXISTS {_q(schema)}"))
        conn.execute(sa.text(ddl))


# ── mysqlsh import ------------------------------------------------------------
def _mysqlsh_import(path: Path, *, host: str, port: int,
                    schema: str, table: str,
                    columns: list[str], dialect: str,
                    threads: int, skip_rows: int,
                    replace_duplicates: bool) -> None:
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
    if replace_duplicates:
        cmd.append("--onDuplicateKeyUpdate")
    subprocess.run(cmd, check=True)


# ── public API ----------------------------------------------------------------
def load_csv(*, csv_path: str | Path, table: str,
             schema: str | None = None, host: str | None = None,
             port: int | None = None, header: bool = True,
             dialect: str = "csv-unix", threads: int = 8,
             replace_duplicates: bool = False, clean: bool = True) -> None:
    """
    Clean (optional), infer schema, create table, and bulk-load CSV via mysqlsh.
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

    header = _detect_header(src, header)
    col_types = _infer_with_arrow(src, header=header)
    col_types = _safe_names(col_types)           # ← guarantee legal names

    _create_table(host=host, port=port,
                  schema=schema, table=table,
                  col_types=col_types)

    _mysqlsh_import(
        src, host=host, port=port,
        schema=schema, table=table,
        columns=list(col_types.keys()),
        dialect=dialect, threads=threads,
        skip_rows=1 if header else 0,
        replace_duplicates=replace_duplicates,
    )

    print(f"[load_csv] Imported {src.name} → {schema}.{table} "
          f"({len(col_types)} columns)")
