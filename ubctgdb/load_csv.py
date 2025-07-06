from __future__ import annotations

# --------------------------------------------------------------------------- #
#  Load .env variables early — even when running under plain Python           #
# --------------------------------------------------------------------------- #
try:
    from dotenv import load_dotenv
    # Loads variables from the nearest `.env`. Falls back silently if the
    # package is not installed (common in the mysqlsh-embedded interpreter).
    load_dotenv()
except ModuleNotFoundError:
    # mysqlsh’s Python often lacks external packages; keep going and rely on
    # env vars provided by the shell / CI instead.
    pass


import csv, math, os, random, time, warnings
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd

try:
    import mysqlsh                    # MySQL Shell ≥8.0
except ModuleNotFoundError as exc:    # graceful failure if user hasn’t installed it
    mysqlsh = None

# --------------------------------------------------------------------------- #
# 1.  helpers
# --------------------------------------------------------------------------- #
_PANDAS2MYSQL = {
    "int64":      "BIGINT",
    "float64":    "DOUBLE",
    "object":     "TEXT",
    "datetime64[ns]": "DATETIME",
}

_SQL_MODE_LAX = (
    "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)

def _map_dtype(series: pd.Series) -> str:
    """
    Rough but serviceable mapping from pandas dtypes to MySQL column types.
    Guesses DECIMAL(p,s) for floats with few decimals.
    """
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        # decide between DECIMAL and DOUBLE
        dp = series.dropna().apply(lambda x: abs(x - round(x))).max()
        dp = int(round(-math.log10(dp), 0)) if dp else 0
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
                       duplicate_mode: str = "ignore",
) -> str:
    """
    Build a CREATE TABLE IF NOT EXISTS statement compatible with util.import_table.
    """
    cols = ",\n  ".join(f"`{c}` {t}" for c, t in col_types.items())
    pk   = f",\n  PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
    eng  = "ENGINE = InnoDB"
    return (
        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` (\n  "
        f"{cols}{pk}\n) {eng};"
    )


# --------------------------------------------------------------------------- #
# 0-bis. Build a MySQL-Shell URI from your .env variables
# --------------------------------------------------------------------------- #
def _mysqlx_uri_from_env() -> str:
    """
    Construct a mysqlx://user:pass@host:port URI from DB_* variables
    (falls back to port 33060 unless DB_PORT is set).
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


# --------------------------------------------------------------------------- #
# 2.  Public entry point
# --------------------------------------------------------------------------- #
def load_csv(
    csv_path: str | Path,
    *,
    table: str,
    schema: str | None = None,
    primary_keys: Sequence[str] | None = None,
    col_types: Mapping[str, str] | None = None,
    duplicate_mode: str = "ignore",             # "ignore" | "replace"
    sql_mode_lax: str = _SQL_MODE_LAX,
    sample_rows: int = 20_000,
    dtype_overrides: Mapping[str, str] | None = None,
    parse_dates: Sequence[str] | None = None,
    retries: int = 5,
    show_progress: bool = True,
    threads: int = 8,
) -> None:
    """
    High-level wrapper around MySQL-Shell util.import_table().

    Parameters
    ----------
    csv_path : local path to the CSV file to ingest
    table    : destination table name (will be auto-created if missing)
    schema   : destination schema; defaults to DB_NAME in .env
    … plus various tuning knobs documented in the README
    """
    csv_path = Path(csv_path).expanduser().resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)

    if mysqlsh is None:
        raise RuntimeError(
            "load_csv() needs the mysql-shell Python module. "
            "Install MySQL Shell 8+ and ensure the script runs either "
            "inside `mysqlsh --py` or spawns mysqlsh via subprocess."
        )

    # ------------------------------------------------------------------ #
    # A.  infer schema if the caller didn't provide one explicitly
    # ------------------------------------------------------------------ #
    if col_types is None:
        print(f"Scanning first {sample_rows:,} rows to infer schema…")
        col_types = _infer_schema(
            csv_path, sample_rows, dtype_overrides, parse_dates
        )
        print("Inferred schema:")
        for col, typ in col_types.items():
            print(f"  {col:<10} {typ}")

    if schema is None:
        schema = os.getenv("DB_NAME") or "public"

    # ------------------------------------------------------------------ #
    # B.  create table if it doesn’t exist
    # ------------------------------------------------------------------ #
    create_sql = _render_create_sql(schema, table, col_types,
                                    primary_keys, duplicate_mode)

    # We rely on sqlalchemy only for the DDL part to avoid manual parsing.
    import sqlalchemy as sa
    engine_url = sa.engine.url.URL.create(
        drivername="mysql+mysqldb",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "3306"),
        database=schema,
    )
    with sa.create_engine(engine_url).begin() as conn:
        conn.execute(sa.text(create_sql))

    # ------------------------------------------------------------------ #
    # C.  use MySQL-Shell for the heavy lifting
    # ------------------------------------------------------------------ #
    uri = _mysqlx_uri_from_env()

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
            "skipRows"          : 1,
            "columns"           : uservars,
            "decodeColumns"     : decode,
            "onDuplicateKeyUpdate": duplicate_mode == "replace",
            "threads"           : threads,
        }

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
