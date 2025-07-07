r"""
High-speed, version-proof CSV loader for MySQL.

Key ideas
---------
* A streaming pre-clean pass turns blank cells and the string "NaN"
  into the literal token ``\N`` that MySQL bulk loaders treat as NULL.
* The ensuing LOAD DATA LOCAL INFILE can map every field directly onto
  its column—no expressions, no user variables—so it works on **any**
  MySQL Shell 8.x build and is as fast as the wire allows.
"""
from __future__ import annotations

import os
import sys
import subprocess
import tempfile
import csv
from collections import OrderedDict
from pathlib import Path
from typing import Mapping, Optional

import pandas as pd
import sqlalchemy as sa
from dotenv import find_dotenv, load_dotenv

# ---------------------------------------------------------------------------
#  Environment
# ---------------------------------------------------------------------------

load_dotenv(find_dotenv(usecwd=True), override=False)
DB_PORT = int(os.getenv("DB_PORT", 3306))

# ---------------------------------------------------------------------------
#  CSV pre-cleaner (streaming – O(1) memory)
# ---------------------------------------------------------------------------


def _clean_csv_to_temp(src_path: Path) -> Path:
    fd, tmp_name = tempfile.mkstemp(
        suffix=".csv", prefix="clean_", dir=src_path.parent
    )
    os.close(fd)  # we'll reopen with csv module

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
    """
    Wrapper around `mysqlsh util.importTable`.  Assumes *csv_path* already
    contains '\N' for NULL, so we can map fields directly onto columns.
    """
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
import sys
try:
    util.import_table(
        r'{csv_path.as_posix()}',
        {options}
    )
    print("Import successful.")
except Exception as e:
    print(f"ERROR: {{e}}", file=sys.stderr)
    sys.exit(1)
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

    if dotenv_path:
        load_dotenv(dotenv_path, override=False)

    host = os.getenv("DB_HOST")
    if not host:
        raise EnvironmentError("DB_HOST not set; check your .env or pass dotenv_path.")

    csv_path = Path(csv_path).expanduser().resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)

    # ------------------------------------------------------------------
    # Optional pre-clean pass
    # ------------------------------------------------------------------
    if preprocess:
        print(
            f"[clean] Streaming {csv_path.name} → null-safe temp file…", file=sys.stderr
        )
        csv_path = _clean_csv_to_temp(csv_path)
        print(f"[clean] Done.  Temp file at {csv_path}", file=sys.stderr)

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
        columns=list(col_types.keys()),
        dialect=dialect,
        threads=threads,
        skip_rows=skip_rows,
        replace_duplicates=replace_duplicates,
    )
