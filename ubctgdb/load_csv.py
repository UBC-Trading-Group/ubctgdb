from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional

import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv

# --------------------------------------------------------------------------- #
#  Environment / configuration
# --------------------------------------------------------------------------- #

load_dotenv()  # DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT (optional)

DB_PORT = int(os.getenv("DB_PORT", 3306))


# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #

_SQL_TYPE_MAP = {
    "int64": "BIGINT",
    "Int64": "BIGINT",
    "float64": "DOUBLE",
    "object": "TEXT",
    "string": "TEXT",
    "datetime64[ns]": "DATETIME",
    "bool": "TINYINT(1)",
}


def _infer_col_types(csv_path: Path, n_rows: int = 20_000) -> "OrderedDict[str, str]":
    """Read *n_rows* from *csv_path* and guess SQL column types."""
    sample = pd.read_csv(csv_path, nrows=n_rows)
    col_types: "OrderedDict[str, str]" = OrderedDict()
    for col, dtype in sample.dtypes.items():
        sql_type = _SQL_TYPE_MAP.get(str(dtype), "TEXT")
        col_types[col] = sql_type
    return col_types


def _sqlalchemy_engine() -> sa.engine.Engine:
    """Return an SQLAlchemy engine (MySQL, PyMySQL driver)."""
    url = sa.engine.url.URL.create(
        drivername="mysql+pymysql",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=DB_PORT,
    )
    return sa.create_engine(url, pool_recycle=1800, pool_pre_ping=True)


def _format_mysql_identifier(name: str) -> str:
    """Escape a MySQL identifier with back‑ticks."""
    if "`" in name:
        raise ValueError("Back‑tick found in identifier: %s" % name)
    return f"`{name}`"


def _create_table(
    schema: str,
    table: str,
    col_types: Mapping[str, str],
    *,
    if_not_exists: bool = True,
) -> None:
    """Create *schema.table* using *col_types* (ordered)."""
    cols_sql = ",\n  ".join(f"{_format_mysql_identifier(c)} {t}" for c, t in col_types.items())
    stmt = f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}{_format_mysql_identifier(schema)}.{_format_mysql_identifier(table)} (\n  {cols_sql}\n) ENGINE=InnoDB;"
    with _sqlalchemy_engine().begin() as conn:
        conn.execute(sa.text(f"CREATE DATABASE IF NOT EXISTS {_format_mysql_identifier(schema)};"))
        conn.execute(sa.text(stmt))


def _run_mysqlsh_import(
    csv_path: Path,
    schema: str,
    table: str,
    columns: Iterable[str],
    *,
    dialect: str = "csv-unix",
    threads: int = 4,
    skip_rows: int = 0,
    replace_duplicates: bool = False,
) -> None:
    """Invoke `mysqlsh util import-table` via subprocess."""
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    uri = f"mysql://{user}:{password}@{host}:{DB_PORT}"

    cmd: List[str] = [
        "mysqlsh",
        uri,
        "--",
        "util",
        "import-table",
        str(csv_path),
        f"--schema={schema}",
        f"--table={table}",
        f"--dialect={dialect}",
        f"--threads={threads}",
        f"--skipRows={skip_rows}",
    ]
    if columns:
        cmd.append(f"--columns={','.join(columns)}")
    if replace_duplicates:
        cmd.append("--replaceDuplicates")

    # For security, replace password in printed command with ***
    printable_cmd = " ".join(shlex.quote(p) if p != uri else uri.replace(password, "***") for p in cmd)
    print("[mysqlsh]", printable_cmd, file=sys.stderr)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            "mysqlsh import failed:\nSTDOUT:\n" + result.stdout + "\nSTDERR:\n" + result.stderr
        )
    print(result.stdout, file=sys.stderr)


# --------------------------------------------------------------------------- #
#  Public API
# --------------------------------------------------------------------------- #

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
) -> None:
    """Bulk‑load *csv_path* into *schema.table* using MySQL Shell.

    Parameters
    ----------
    csv_path:
        Path to the input CSV file.
    table:
        Target table name.
    schema:
        Database name. Defaults to ``DB_NAME`` from environment.
    col_types:
        Explicit `{column: sql_type}` mapping. If *None*, a schema is
        inferred from the first *infer_rows* rows of the CSV.
    infer_rows:
        Number of rows to scan when inferring types (ignored if
        *col_types* provided).
    dialect:
        One of ``default``, ``csv``, ``csv-unix``, ``tsv``, or ``json``.
    threads:
        Number of concurrent load threads.
    skip_rows:
        Rows to skip at file start (e.g. header row).
    replace_duplicates:
        If ``True``, conflicting primary/unique keys are *replaced* instead
        of being skipped. Maps to ``--replaceDuplicates`` in mysqlsh.
    """

    csv_path = Path(csv_path).expanduser().resolve()
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    schema = schema or os.getenv("DB_NAME")
    if not schema:
        raise ValueError("Schema not provided and DB_NAME env var is not set.")

    # Infer schema if necessary
    if col_types is None:
        col_types = _infer_col_types(csv_path, n_rows=infer_rows)

    # Ensure table exists
    _create_table(schema, table, col_types, if_not_exists=True)

    # Launch mysqlsh import
    _run_mysqlsh_import(
        csv_path=csv_path,
        schema=schema,
        table=table,
        columns=col_types.keys(),
        dialect=dialect,
        threads=threads,
        skip_rows=skip_rows,
        replace_duplicates=replace_duplicates,
    )
