from __future__ import annotations

import csv
import os
import re
import subprocess
import sys
import tempfile
from collections import OrderedDict
from pathlib import Path
from typing import Mapping, Optional

import pandas as pd
import sqlalchemy as sa
from dotenv import find_dotenv, load_dotenv

# -----------------------------------------------------------------------------
#  Environment
# -----------------------------------------------------------------------------
dotenv_path = find_dotenv(usecwd=True)  # <-- CRITICAL CHANGE
if dotenv_path:
    load_dotenv(dotenv_path, override=False)
else:
    sys.stderr.write(
        "[load_csv] WARNING: .env not found via find_dotenv(); expecting "
        "DB_* variables in the process environment.\n"
    )

DEFAULT_DB_PORT = int(os.getenv("DB_PORT", "3306"))

# -----------------------------------------------------------------------------
#  CSV pre-cleaner  (streaming – O(1) memory)
# -----------------------------------------------------------------------------


def _clean_csv_to_temp(src_path: Path) -> Path:
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

        for row in reader:
            writer.writerow(
                [
                    r"\N" if cell.strip() in {"", "nan", "NaN", "NULL"} else cell
                    for cell in row
                ]
            )

    return tmp_path


# -----------------------------------------------------------------------------
#  Datatype inference  (streaming)
# -----------------------------------------------------------------------------


_INT_LIMITS = [
    ("TINYINT", -128, 127, 255),
    ("SMALLINT", -32768, 32767, 65535),
    ("MEDIUMINT", -8388608, 8388607, 16777215),
    ("INT", -2147483648, 2147483647, 4294967295),
    ("BIGINT", -9223372036854775808, 9223372036854775807, 18446744073709551615),
]


def _infer_column_types(csv_path: Path, delimiter: str = ",") -> OrderedDict[str, str]:
    meta = None  # will become list[dict] once header known

    date_re = re.compile(r"\d{4}-\d{2}-\d{2}$")
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader)
        meta = [
            {
                "max_len": 0,
                "has_lz": False,
                "is_int": True,
                "is_decimal": True,
                "min_int": 0,
                "max_int": 0,
                "max_scale": 0,
            }
            for _ in header
        ]

        for row in reader:
            for idx, cell in enumerate(row):
                if cell in {"", r"\N"}:
                    # treat NULL as no-info
                    continue

                m = meta[idx]
                m["max_len"] = max(m["max_len"], len(cell))

                # leading zero?
                if len(cell) > 1 and cell[0] == "0":
                    m["has_lz"] = True

                # quick date check (YYYY-MM-DD)
                # defer – we’ll tag as DATE later

                # numeric analysis
                if m["is_int"] or m["is_decimal"]:
                    if cell.isdigit():
                        # integer, possibly large
                        val = int(cell)
                        if m["is_int"]:
                            if m["min_int"] == m["max_int"] == 0:
                                m["min_int"] = m["max_int"] = val
                            else:
                                m["min_int"] = min(m["min_int"], val)
                                m["max_int"] = max(m["max_int"], val)
                    else:
                        # could be decimal?
                        try:
                            # split mantissa.scale ourselves to keep precision
                            if "." in cell:
                                left, right = cell.split(".", 1)
                                if left.lstrip("+-").isdigit() and right.isdigit():
                                    m["max_scale"] = max(m["max_scale"], len(right))
                                    m["is_int"] = False
                                    val_int = int(left or "0")
                                    m["min_int"] = min(m["min_int"], val_int)
                                    m["max_int"] = max(m["max_int"], val_int)
                                else:
                                    m["is_int"] = m["is_decimal"] = False
                            else:
                                m["is_int"] = m["is_decimal"] = False
                        except Exception:
                            m["is_int"] = m["is_decimal"] = False

    col_types: OrderedDict[str, str] = OrderedDict()

    for col, m in zip(header, meta):
        # DATE first
        sample_len = m["max_len"]
        if sample_len == 10 and m["is_int"] is False and date_re.match("0" * 10):
            # This rough check alone isn't enough; do a cheap final test on header row value
            col_types[col] = "DATE"
            continue

        # keep leading zeroes?
        if m["has_lz"]:
            col_types[col] = f"CHAR({sample_len})"
            continue

        # integers?
        if m["is_int"] and sample_len <= 20:
            signed_ok = False
            unsigned_ok = m["min_int"] >= 0
            for name, lo, hi, uhi in _INT_LIMITS:
                if unsigned_ok and m["max_int"] <= uhi:
                    col_types[col] = f"{name} UNSIGNED"
                    signed_ok = True
                    break
                if not unsigned_ok and lo <= m["min_int"] and m["max_int"] <= hi:
                    col_types[col] = name
                    signed_ok = True
                    break
            if signed_ok:
                continue

        # decimals?
        if m["is_decimal"]:
            precision = len(str(m["max_int"])) + m["max_scale"]
            if precision <= 38:  # MySQL DECIMAL max is 65, but 38 keeps it portable
                col_types[col] = f"DECIMAL({precision},{m['max_scale']})"
            else:
                col_types[col] = "DOUBLE"
            continue

        # fallback – string
        if sample_len > 255:
            col_types[col] = "TEXT"
        else:
            col_types[col] = f"VARCHAR({sample_len})"

    return col_types


# -----------------------------------------------------------------------------
#  SQL helpers
# -----------------------------------------------------------------------------


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


def _q(name: str) -> str:  # quote identifier safely
    if "`" in name:
        raise ValueError(f"Back-tick in identifier: {name}")
    return f"`{name}`"


def _create_table(
    host: str,
    port: int,
    schema: str,
    table: str,
    col_types: Mapping[str, str],
    if_not_exists: bool = True,
) -> None:
    cols_sql = ",\n  ".join(f"{_q(c)} {t}" for c, t in col_types.items())
    ddl = (
        f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}"
        f"{_q(schema)}.{_q(table)} (\n  {cols_sql}\n) ENGINE=InnoDB;"
    )
    with _sqlalchemy_engine(host, port).begin() as conn:
        conn.execute(sa.text(f"CREATE DATABASE IF NOT EXISTS {_q(schema)};"))
        conn.execute(sa.text(ddl))


def _run_mysqlsh_import(
    *,
    csv_path: Path,
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
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")

    uri = f"mysql://{user}:{password}@{host}:{port}"
    cmd = [
        "mysqlsh",
        uri,
        "--",
        "util",
        "import-table",
        str(csv_path),
        f"--schema={schema}",
        f"--table={table}",
        f"--columns={','.join(columns)}",
        f"--dialect={dialect}",
        f"--threads={threads}",
        f"--skipRows={skip_rows}",
    ]
    if replace_duplicates:
        cmd.append("--onDuplicateKeyUpdate")
    subprocess.run(cmd, check=True)


# -----------------------------------------------------------------------------
#  Public API
# -----------------------------------------------------------------------------


def load_csv(
    *,
    csv_path: str | Path,
    table: str,
    schema: str | None = None,
    host: str | None = None,
    port: int | None = None,
    col_types: Mapping[str, str] | None = None,
    dialect: str = "csv-unix",
    threads: int = 8,
    skip_rows: int = 1,
    replace_duplicates: bool = False,
) -> None:
    csv_path = Path(csv_path).expanduser()
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    schema = schema or os.getenv("DB_NAME")
    host = host or os.getenv("DB_HOST")
    port = port or DEFAULT_DB_PORT

    if schema is None or host is None:
        raise RuntimeError("DB_HOST and DB_NAME must be set (env or argument)")

    # 1. Clean NaNs/empties to \N for LOAD DATA
    cleaned_path = _clean_csv_to_temp(csv_path)

    # 2. Column types
    if col_types is None:
        col_types = _infer_column_types(csv_path)

    # 3. Create table if needed
    _create_table(
        host=host,
        port=port,
        schema=schema,
        table=table,
        col_types=col_types,
        if_not_exists=True,
    )

    # 4. Import via mysqlsh
    _run_mysqlsh_import(
        csv_path=cleaned_path,
        host=host,
        port=port,
        schema=schema,
        table=table,
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
