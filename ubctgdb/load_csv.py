from __future__ import annotations
from .core import _engine  

import csv, math, os, random, time, warnings
from pathlib import Path
from typing import Mapping, Sequence

import sqlalchemy as sa
import pandas as pd

try:
    import mysqlsh                    # MySQL Shell ≥8.0
except ModuleNotFoundError as exc:    # graceful failure if user hasn’t installed it
    mysqlsh = None

# --------------------------------------------------------------------------- #
# 1.  helpers
# --------------------------------------------------------------------------- #
_SQL_MODE_LAX = "NO_ENGINE_SUBSTITUTION"

_PANDAS2MYSQL = {                      # dtype → default MySQL column
    "int64"   : "BIGINT",
    "float64" : "DOUBLE",
    "object"  : "TEXT",
    "bool"    : "TINYINT(1)",
    "datetime64[ns]" : "DATETIME",
    "timedelta64[ns]": "BIGINT",
}

def _map_dtype(series: pd.Series) -> str:
    """Convert a pandas dtype to a *reasonable* MySQL type."""
    if pd.api.types.is_integer_dtype(series):
        maxabs = series.dropna().abs().max()
        if maxabs < 2**7:     return "TINYINT"
        if maxabs < 2**15:    return "SMALLINT"
        if maxabs < 2**31:    return "INT"
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        # guess DECIMAL if <= 6 dp and magnitude reasonable, else DOUBLE
        frac, _ = math.modf(series.dropna().iloc[0]) if not series.dropna().empty else (0.0, 0.0)
        dp = len(str(frac).split(".")[1]) if frac else 0
        if dp <= 6:
            width = max(len(f"{v:.0f}") for v in series.dropna()) + dp + 1
            return f"DECIMAL({width},{dp})"
        return "DOUBLE"
    return _PANDAS2MYSQL.get(str(series.dtype), "TEXT")


def _infer_schema(csv_file: str | Path,
                  sample_rows: int = 20_000,
                  dtype_overrides: Mapping[str, str] | None = None,
                  parse_dates: Sequence[str] | None = None,
) -> Mapping[str, str]:
    """
    Peek at the first N rows to guess sensible column ↦ MySQL type mapping.
    dtype_overrides lets the caller force/override the guess.
    """
    sample = pd.read_csv(csv_file,
                         nrows=sample_rows,
                         parse_dates=parse_dates,
                         low_memory=False)
    schema = {col: _map_dtype(sample[col]) for col in sample.columns}
    if dtype_overrides:
        schema.update(dtype_overrides)
    return schema


def _render_create_sql(schema: str,
                       table: str,
                       col_types: Mapping[str, str],
                       primary_keys: Sequence[str] | None = None,
                       engine: str = "InnoDB",
                       charset: str = "utf8mb4",
                       collate: str = "utf8mb4_0900_ai_ci",
) -> str:
    pk_sql = f",\n    PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
    cols   = ",\n    ".join(f"{name} {ctype} NULL" for name, ctype in col_types.items())
    return f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
    {cols}{pk_sql}
) ENGINE={engine}
  DEFAULT CHARSET={charset}
  COLLATE={collate};"""


# --------------------------------------------------------------------------- #
# 2.  public helper
# --------------------------------------------------------------------------- #
def load_csv(
    csv_path: str | Path,
    table: str,
    *,
    schema: str | None = None,
    infer_schema: bool = True,
    col_types: Mapping[str, str] | None = None,
    primary_keys: Sequence[str] | None = None,
    skip_rows: int = 1,
    threads: int = 8,
    chunk_size: int = 200 * 1024 * 1024,
    duplicate_mode: str = "ignore",             # "ignore" | "replace"
    sql_mode_lax: str = _SQL_MODE_LAX,
    sample_rows: int = 20_000,
    dtype_overrides: Mapping[str, str] | None = None,
    parse_dates: Sequence[str] | None = None,
    retries: int = 5,
    show_progress: bool = True,
) -> None:
    """
    High-level wrapper around MySQL-Shell util.import_table().

    Parameters
    ----------
    csv_path        : local path to the CSV file to ingest
    table, schema   : destination table (schema defaults to current DB if None)
    infer_schema    : if True, pandas samples first *sample_rows* rows to guess column types
    col_types       : dict of {column_name: mysql_column_type}. Overrides inference
    primary_keys    : columns to declare PRIMARY KEY. Optional
    duplicate_mode  : "ignore" (default)  = INSERT IGNORE
                      "replace"           = REPLACE INTO (overwrites duplicates)
    dtype_overrides : dict of column → mysql_type that always wins (helpful for decimals)
    parse_dates     : list of columns that should be parsed as dates while inferring
    """
    if mysqlsh is None:
        raise RuntimeError("load_csv() needs the mysql-shell Python module "
                           "(pip install mysql-shell).")

    csv_path = Path(csv_path).expanduser().resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)

    # ------------------------------------------------------------------ #
    # build column → mysql_type mapping
    # ------------------------------------------------------------------ #
    if infer_schema and col_types is None:
        col_types = _infer_schema(csv_path,
                                  sample_rows=sample_rows,
                                  dtype_overrides=dtype_overrides,
                                  parse_dates=parse_dates)
    elif col_types is None:
        raise ValueError("Either infer_schema=True or col_types must be provided")

    # ------------------------------------------------------------------ #
    # create table if necessary
    # ------------------------------------------------------------------ #
    if schema is None:
        schema = os.getenv("DB_NAME") or "public"

    create_sql = _render_create_sql(schema, table, col_types,
                                    primary_keys=primary_keys)
    with _engine() as eng:
        with eng.begin() as conn:
            conn.execute(sa.text(create_sql))
    
    # --------------------------------------------------------------------------- #
    # Build a MySQL-Shell URI from  .env variables
    # --------------------------------------------------------------------------- #
    def _mysqlx_uri_from_env() -> str:
        """
        Construct a “mysqlx://user:pass@host:port” URI from the standard
        DB_* variables already in your .env. Falls back to port 33060
        (the default X-Protocol port) unless DB_PORT is set.
        """
        host   = os.getenv("DB_HOST")
        user   = os.getenv("DB_USER")
        passwd = os.getenv("DB_PASS")
        port   = os.getenv("DB_PORT", "33060")

        if not all([host, user, passwd]):
            raise EnvironmentError(
                "DB_HOST, DB_USER and DB_PASS must be defined in your .env "
                "to build the MySQL-Shell connection URI."
            )
        return f"mysqlx://{user}:{passwd}@{host}:{port}"


    # ------------------------------------------------------------------ #
    # use MySQL-Shell for the heavy lifting
    # ------------------------------------------------------------------ #
    uri = _mysqlx_uri_from_env()     
    if not uri:
        raise EnvironmentError("check .env")

    sess = mysqlsh.mysql.get_session(uri)
    try:
        sess.run_sql(f"SET @@session.sql_mode = '{sql_mode_lax}';")

        # column list – encode all to user-variables first so we can NULLIF('')
        uservars = [f"@{c}" for c in col_types]
        decode   = {col: f"NULLIF(@{col}, '')" for col in col_types}

        import_cfg = {
            "schema"            : schema,
            "table"             : table,
            "dialect"           : "csv-unix",
            "skipRows"          : skip_rows,
            "columns"           : uservars,
            "decodeColumns"     : decode,
            "fieldsTerminatedBy": ",",
            "fieldsEnclosedBy"  : '"',
            "threads"           : threads,
            "chunkSize"         : chunk_size,
            "showProgress"      : show_progress,
            "sessionVariables"  : {
                "net_read_timeout"  : 3600,
                "net_write_timeout" : 3600,
                "wait_timeout"      : 3600
            }
        }
        if duplicate_mode == "ignore":
            import_cfg["ignore"] = True
        elif duplicate_mode == "replace":
            import_cfg["replaceDuplicates"] = True
        else:
            raise ValueError("duplicate_mode must be 'ignore' or 'replace'")

        # retry with exponential back-off
        for attempt in range(1, retries + 1):
            try:
                mysqlsh.util.import_table(str(csv_path), import_cfg)
                if show_progress:
                    print("✅  import finished")
                break
            except Exception as err:
                if attempt == retries:
                    raise
                wait = 2 ** attempt + random.random()
                warnings.warn(f"Attempt {attempt} failed: {err}. Retrying in "
                              f"{wait:.1f}s …", RuntimeWarning)
                time.sleep(wait)
    finally:
        sess.close()
