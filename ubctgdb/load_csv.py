from __future__ import annotations

import os
import re
import sys
import csv
import subprocess
import tempfile
from collections import OrderedDict
from pathlib import Path
from typing import Mapping, Optional

import pandas as pd
import sqlalchemy as sa
from dotenv import find_dotenv, load_dotenv

# ---------------------------------------------------------------------------
#  Environment
# ---------------------------------------------------------------------------

# Load variables from a .env in the cwd (or a parent); keep any that are
# already set in the process environment.
load_dotenv(find_dotenv(usecwd=True))
DEFAULT_DB_PORT = 3306

# ---------------------------------------------------------------------------
#  CSV pre-cleaner (streaming – O(1) memory)
# ---------------------------------------------------------------------------


def _clean_csv_to_temp(src_path: Path) -> Path:
    fd, tmp_name = tempfile.mkstemp(
        suffix=".csv", prefix="clean_", dir=src_path.parent
    )
    os.close(fd)  # close the descriptor; we’ll reopen with csv module

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
                    r"\N"
                    if (cell.strip() == "" or cell.strip().lower() == "nan")
                    else cell
                    for cell in row
                ]
            )

    return tmp_path


# ---------------------------------------------------------------------------
#  SQL helpers
# ---------------------------------------------------------------------------


def _sqlalchemy_engine(host: str, port: int) -> sa.engine.Engine:
    url = sa.engine.url.URL.create(
        drivername="mysql+pymysql",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=host,
        port=port,
    )
    return sa.create_engine(url, pool_pre_ping=True, pool_recycle=1800)


def _q(name: str) -> str:  # quote identifier
    if "`" in name:
        raise ValueError("Back-tick in identifier: %s" % name)
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

    options = {
        "schema": schema,
        "table": table,
        "dialect": dialect,
        "threads": threads,
        "skipRows": skip_rows,
        "replaceDuplicates": replace_duplicates,
        "columns": columns,  # 1-to-1 mapping
    }

    py_script = f"""
import util
util.import_table(
    {str(csv_path)!r},
    {options!r},
)
"""

    cmd = ["mysqlsh", uri, "--py", "-e", py_script]

    # scrub password for debug echo
    if password:
        cmd[1] = cmd[1].replace(password, "***")
    print("[mysqlsh] Executing script…", file=sys.stderr)

    res = subprocess.run(cmd, capture_output=True, text=True)

    if res.stdout:
        print(res.stdout, file=sys.stderr)
    if res.stderr:
        print(res.stderr, file=sys.stderr)

    if res.returncode != 0:
        raise RuntimeError("mysqlsh import failed.  See messages above.")


# ---------------------------------------------------------------------------
#  Public API
# ---------------------------------------------------------------------------


def load_csv(
    *,
    csv_path: str | Path,
    table: str,
    schema: Optional[str] = None,
    col_types: Optional[Mapping[str, str]] = None,
    infer_rows: int = 20_000,
    dialect: str = "csv-unix",
    threads: int = 4,
    skip_rows: int = 0,
    replace_duplicates: bool = False,
    preprocess: bool = True,
    dotenv_path: Optional[str | Path] = None,
) -> None:
    """Stream-clean a CSV and load it into MySQL at wire-speed."""

    csv_path = Path(csv_path)

    # Allow the caller to point at a specific .env file
    if dotenv_path:
        load_dotenv(dotenv_path, override=True)

    host = os.getenv("DB_HOST")
    port = int(os.getenv("DB_PORT", DEFAULT_DB_PORT))

    if not host:
        raise EnvironmentError("DB_HOST not set; check your .env or pass dotenv_path")

    schema = schema or os.getenv("DB_NAME")
    if not schema:
        raise ValueError("Schema not provided and DB_NAME env var is not set")

    if preprocess:
        csv_path = _clean_csv_to_temp(csv_path)

    if col_types is None:
        col_types = _infer_col_types(csv_path, infer_rows)

    _create_table(host, port, schema, table, col_types, if_not_exists=True)

    _run_mysqlsh_import(
        csv_path=csv_path,
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


# ---------------------------------------------------------------------------
#  Column-type inference
# ---------------------------------------------------------------------------


def _infer_col_types(csv_path: Path, n_rows: int) -> OrderedDict[str, str]:
    df = pd.read_csv(csv_path, nrows=n_rows)
    types: OrderedDict[str, str] = OrderedDict()
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            types[col] = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            types[col] = "DOUBLE"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            types[col] = "DATETIME"
        else:
            max_len = int(df[col].astype(str).str.len().max())
            types[col] = f"VARCHAR({max_len})"
    return types
