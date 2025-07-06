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
from dotenv import find_dotenv, load_dotenv

# ---------------------------------------------------------------------------
#  Environment
# ---------------------------------------------------------------------------

load_dotenv(find_dotenv(usecwd=True), override=False)
DB_PORT = int(os.getenv("DB_PORT", 3306))

# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

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
    sample = pd.read_csv(csv_path, nrows=n_rows)
    out: "OrderedDict[str, str]" = OrderedDict()
    for col, dtype in sample.dtypes.items():
        out[col] = _SQL_TYPE_MAP.get(str(dtype), "TEXT")
    return out


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
        raise ValueError("Back‑tick in identifier: %s" % name)
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
        f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}{_q(schema)}.{_q(table)} (\n  {cols_sql}\n) ENGINE=InnoDB;"
    )
    with _sqlalchemy_engine(host, port).begin() as conn:
        conn.execute(sa.text(f"CREATE DATABASE IF NOT EXISTS {_q(schema)};"))
        conn.execute(sa.text(ddl))


# ---------------------------------------------------------------------------
#  mysqlsh wrapper
# ---------------------------------------------------------------------------

def _run_mysqlsh_import(
    csv_path: Path,
    host: str,
    port: int,
    schema: str,
    table: str,
    columns: Iterable[str],
    *,
    dialect: str,
    threads: int,
    skip_rows: int,
    replace_duplicates: bool,
    stream: bool,
    empty_as_null: bool,
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

    cols = list(columns)
    if empty_as_null:
        # For each field produce a user variable then set column = NULLIF(@var,'')
        col_spec_parts: List[str] = []
        for c in cols:
            col_spec_parts.append(f"@{c}")
            col_spec_parts.append(f"{c}=NULLIF(@{c},'')")
        cmd.append("--columns=" + ",".join(col_spec_parts))
    else:
        cmd.append("--columns=" + ",".join(cols))

    if replace_duplicates:
        cmd.append("--replaceDuplicates")

    # Show a scrubbed command line
    safe_cmd = " ".join(
        shlex.quote(p.replace(password, "***")) if p.startswith("mysql://") else shlex.quote(p) for p in cmd
    )
    print("[mysqlsh]", safe_cmd, file=sys.stderr)

    # --- Run and stream output ---
    if stream:
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1) as proc:
            assert proc.stdout is not None
            for line in proc.stdout:
                print(line, end="", file=sys.stderr)
            rc = proc.wait()
        if rc != 0:
            raise RuntimeError("mysqlsh import failed – see log above")
    else:
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            raise RuntimeError(
                "mysqlsh import failed:\nSTDOUT:\n" + res.stdout + "\nSTDERR:\n" + res.stderr
            )
        print(res.stdout, file=sys.stderr)


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
    empty_as_null: bool = True,
    dotenv_path: Optional[str | Path] = None,
) -> None:
    """High‑speed CSV loader.

    Parameters
    ----------
    empty_as_null : bool, default False
        Convert **all** empty strings to SQL `NULL` using `NULLIF(@var,'')`.
    """

    if dotenv_path:
        load_dotenv(dotenv_path, override=False)

    host = os.getenv("DB_HOST")
    if not host:
        raise EnvironmentError("DB_HOST is not set; check your .env or pass dotenv_path.")

    csv_path = Path(csv_path).expanduser().resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)

    schema = schema or os.getenv("DB_NAME")
    if not schema:
        raise ValueError("Schema not provided and DB_NAME env var is not set.")

    if col_types is None:
        col_types = _infer_col_types(csv_path, infer_rows)

    _create_table(host, DB_PORT, schema, table, col_types, if_not_exists=True)

    _run_mysqlsh_import(
        csv_path=csv_path,
        host=host,
        port=DB_PORT,
        schema=schema,
        table=table,
        columns=col_types.keys(),
        dialect=dialect,
        threads=threads,
        skip_rows=skip_rows,
        replace_duplicates=replace_duplicates,
        stream=True,
        empty_as_null=empty_as_null,
    )
