
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
import pyarrow as pa
import pyarrow.csv as pacsv
from dotenv import find_dotenv, load_dotenv

# Local shared helpers
from .core import (
    NULL_MARKERS,
    NULL_TOKEN,
    PROGRESS_EVERY,
    q as _q,
    sqlalchemy_engine as _sqlalchemy_engine,
)

# ── env ───────────────────────────────────────────────────────────────────
load_dotenv(find_dotenv(usecwd=True), override=False)
DEFAULT_DB_PORT = int(os.getenv("DB_PORT", "3306"))

# ── clean ────────────────────────────────────────────────────────────────
_NULLS = {x.lower() for x in NULL_MARKERS}

def _clean_inplace(src: Path) -> None:
    """Overwrite *src* replacing empty/NULL markers with ``\N``."""
    print(f"[clean] Overwriting {src.name} …")
    t0 = time.perf_counter()
    fd, tmp = tempfile.mkstemp(suffix=".csv", dir=src.parent, prefix="tmp_")
    os.close(fd)
    tmp_path = Path(tmp)

    with src.open(newline="", encoding="utf-8") as fin, \
         tmp_path.open("w", newline="", encoding="utf-8") as fout:
        readr, writr = csv.reader(fin), csv.writer(fout, lineterminator="\n")
        for i, row in enumerate(readr, 1):
            writr.writerow(
                (NULL_TOKEN if cell.strip().lower() in _NULLS else cell.strip())
                for cell in row
            )
            if i % PROGRESS_EVERY == 0:
                print(f"[clean]   … {i:,} rows")
    os.replace(tmp_path, src)
    print(f"[clean] Done in {time.perf_counter() - t0:.1f} s")

# ── header sniffing ──────────────────────────────────────────────────────
def _looks_like_data(val: str) -> bool:
    return (
        val.replace(".", "", 1).lstrip("-").isdigit()
        or (len(val) == 10 and val[4] == "-" and val[7] == "-")
    )

def _auto_header(path: Path) -> bool:
    first = next(csv.reader([path.read_text().splitlines()[0]]))
    auto = not any(_looks_like_data(x) for x in first)
    print(f"[upload_csv] auto-detect header → {auto}")
    return auto

# ── Arrow schema inference ───────────────────────────────────────────────
def _infer_schema(path: Path, *, header: bool) -> OrderedDict[str, str]:
    print("[infer] PyArrow schema …")
    t0 = time.perf_counter()
    tbl = pacsv.read_csv(
        path,
        read_options=pacsv.ReadOptions(
            autogenerate_column_names=not header,
            skip_rows=0,
        ),
        convert_options=pacsv.ConvertOptions(
            null_values=[NULL_TOKEN, *NULL_MARKERS]
        ),
    )

    out: OrderedDict[str, str] = OrderedDict()
    for name, col in zip(tbl.schema.names, tbl.columns, strict=True):
        t = col.type
        if pa.types.is_boolean(t):
            out[name] = "TINYINT UNSIGNED"
        elif pa.types.is_integer(t):
            mysql = {8: "TINYINT", 16: "SMALLINT", 32: "INT", 64: "BIGINT"}[t.bit_width]
            out[name] = mysql if pa.types.is_signed_integer(t) else f"{mysql} UNSIGNED"
        elif pa.types.is_floating(t):
            out[name] = "DOUBLE"
        elif pa.types.is_decimal(t):
            out[name] = f"DECIMAL({t.precision},{t.scale})" if t.precision <= 38 else "DOUBLE"
        elif pa.types.is_date(t):
            out[name] = "DATE"
        elif pa.types.is_timestamp(t):
            out[name] = "DATETIME"
        elif pa.types.is_string(t) or pa.types.is_binary(t):
            max_len = pa.compute.max(pa.compute.utf8_length(col)).as_py() or 0
            out[name] = "TEXT" if max_len > 255 else f"VARCHAR({max_len})"
        else:
            out[name] = "TEXT"
    print(f"[infer] Done in {time.perf_counter() - t0:.1f} s ({len(out)} cols)")
    return out

# ── column-name sanitiser ────────────────────────────────────────────────
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

# ── DDL helper ───────────────────────────────────────────────────────────
def _create_table(
    host: str, port: int, schema: str, table: str,
    cols: Mapping[str, str], *, replace: bool
) -> None:
    with _sqlalchemy_engine(host=host, port=port).begin() as conn:
        conn.execute(sa.text(f"CREATE DATABASE IF NOT EXISTS {_q(schema)}"))
        if replace:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {_q(schema)}.{_q(table)}"))
        ddl = ",\n  ".join(f"{_q(c)} {t}" for c, t in cols.items())
        conn.execute(sa.text(
            f"CREATE TABLE IF NOT EXISTS {_q(schema)}.{_q(table)} (\n  {ddl}\n) ENGINE=InnoDB;"
        ))

# ── mysqlsh wrapper ──────────────────────────────────────────────────────
def _mysqlsh(
    path: Path, *, host: str, port: int, schema: str, table: str,
    columns: list[str], dialect: str, threads: int,
    skip_rows: int, replace_dup: bool
) -> None:
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
        cmd.append("--replaceDuplicates")
    subprocess.run(cmd, check=True)

# ── public API ───────────────────────────────────────────────────────────
def upload_csv(
    *,
    csv_path: str | Path,
    table: str,
    schema: str | None = None,
    host: str | None = None,
    port: int | None = None,
    header: bool | None = None,       
    dialect: str = "csv-unix",
    threads: int = 8,
    replace_duplicates: bool = False,
    clean: bool = True,
    replace_table: bool = False,
) -> None:
    """
    Bulk-load *csv_path* into MySQL using MySQL Shell (fast).

    Missing values are normalised to ``\\N`` so that:
    • pandas/Arrow see NaN / pd.NA (we register the token in _infer_schema)
    • MySQL util.importTable() sees SQL NULL.
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

    types = _safe_names(_infer_schema(src, header=header))
    _create_table(host, port, schema, table, types, replace=replace_table)

    _mysqlsh(
        src, host=host, port=port, schema=schema, table=table,
        columns=list(types.keys()),
        dialect=dialect, threads=threads,
        skip_rows=1 if header else 0,
        replace_dup=replace_duplicates,
    )

    print(f"[upload_csv] Imported {src.name} → {schema}.{table} "
          f"({len(types)} columns)")
