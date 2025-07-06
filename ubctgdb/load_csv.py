from __future__ import annotations

import os
import shlex
import subprocess
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Iterable, List, Mapping, Optional

import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv(usecwd=True), override=False)

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
    """Scan *n_rows* of *csv_path* and guess reasonable MySQL column types."""
    sample = pd.read_csv(csv_path, nrows=n_rows)
    col_types: "OrderedDict[str, str]" = OrderedDict()
    for col, dtype in sample.dtypes.items():
        col_types[col] = _SQL_TYPE_MAP.get(str(dtype), "TEXT")
    return col_types


def _sqlalchemy_engine(host: str, port: int) -> sa.engine.Engine:
    """Return an SQLAlchemy engine using PyMySQL."""
    url = sa.engine.url.URL.create(
        drivername="mysql+pymysql",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=host,
        port=port,
    )
    return sa.create_engine(url, pool_recycle=1800, pool_pre_ping=True)


def _format_mysql_identifier(name: str) -> str:
    if "`" in name:
        raise ValueError("Back‑tick found in identifier: %s" % name)
    return f"`{name}`"


def _create_table(
    host: str,
    port: int,
    schema: str,
    table: str,
    col_types: Mapping[str, str],
    *,
    if_not_exists: bool = True,
) -> None:
    cols_sql = ",\n  ".join(f"{_format_mysql_identifier(c)} {t}" for c, t in col_types.items())
    stmt = (
        f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}"
        f"{_format_mysql_identifier(schema)}.{_format_mysql_identifier(table)} (\n  {cols_sql}\n) ENGINE=InnoDB;"
    )
    with _sqlalchemy_engine(host, port).begin() as conn:
        conn.execute(sa.text(f"CREATE DATABASE IF NOT EXISTS {_format_mysql_identifier(schema)};"))
        conn.execute(sa.text(stmt))


def _run_mysqlsh_import(
    csv_path: Path,
    host: str,
    port: int,
    schema: str,
    table: str,
    columns: Iterable[str],
    *,
    dialect: str = "csv-unix",
    threads: int = 4,
    skip_rows: int = 0,
    replace_duplicates: bool = False,
) -> None:
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")

    uri = f"mysql://{user}:{password}@{host}:{port}"

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
        cmd.append("--columns=" + ",".join(columns))
    if replace_duplicates:
        cmd.append("--replaceDuplicates")

    printable_cmd = " ".join(
        shlex.quote(part.replace(password, "***")) if password and part.startswith("mysql://") else shlex.quote(part)
        for part in cmd
    )
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
    dotenv_path: Optional[str | Path] = None,
) -> None:
    """Bulk‑load *csv_path* into *schema.table* through MySQL Shell.

    Parameters
    ----------
    csv_path : str | Path
        Path to the CSV file on disk.
    table : str
        Target table name.
    schema : str | None
        Database name (defaults to ``DB_NAME`` from env).
    col_types : Mapping[str, str] | None
        Explicit column‑type mapping.  If *None* we infer from the first
        *infer_rows* rows.
    infer_rows : int
        Sample size for type inference.
    dialect : str
        csv‑unix | csv | tsv | json
    threads : int
        Parallel load threads.
    skip_rows : int
        Lines to skip at top of file (e.g. header row).
    replace_duplicates : bool
        Passes `--replaceDuplicates` to `mysqlsh`.
    dotenv_path : str | Path | None
        Explicit path to a .env; overrides auto‑detection.
    """

    # Reload env if an explicit dotenv_path is supplied.
    if dotenv_path:
        load_dotenv(dotenv_path, override=False)

    host = os.getenv("DB_HOST")
    port = DB_PORT

    if not host:
        raise EnvironmentError(
            "DB_HOST not set.  Either export it, add it to .env, or pass "
            "dotenv_path=`…` so we can load it."
        )

    csv_path = Path(csv_path).expanduser().resolve()
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    schema = schema or os.getenv("DB_NAME")
    if not schema:
        raise ValueError("Schema not provided and DB_NAME env var is not set.")

    # Infer SQL types if the user didn’t supply them
    if col_types is None:
        col_types = _infer_col_types(csv_path, n_rows=infer_rows)

    # Ensure target table exists
    _create_table(host, port, schema, table, col_types, if_not_exists=True)

    # Perform high‑speed import via mysqlsh
    _run_mysqlsh_import(
        csv_path=csv_path,
        host=host,
        port=port,
        schema=schema,
        table=table,
        columns=col_types.keys(),
        dialect=dialect,
        threads=threads,
        skip_rows=skip_rows,
        replace_duplicates=replace_duplicates,
    )
