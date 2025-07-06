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
import json
import shutil
import subprocess
import textwrap
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd

try:
    import mysqlsh                    # MySQL Shell ≥8.0
except ModuleNotFoundError:           # graceful failure if user hasn’t installed it
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
        # Use a small epsilon to avoid float precision issues with log10
        series_no_na = series.dropna()
        if series_no_na.empty:
            return "DOUBLE"  # Default if all are NaN
        dp = series_no_na.apply(lambda x: abs(x - round(x))).max()
        dp = int(round(-math.log10(dp), 0)) if dp > 1e-9 else 0
        if dp <= 6:
            width = max(len(f"{v:.0f}") for v in series_no_na.dropna()) + dp
            # Add 1 for the decimal point if there are decimal places
            width += 1 if dp > 0 else 0
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
    pk   = f",\n  PRIMARY KEY ({', '.join(f'`{k}`' for k in primary_keys)})" if primary_keys else ""
    eng  = "ENGINE = InnoDB"
    return (
        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` (\n  "
        f"{cols}{pk}\n) {eng};"
    )


# --------------------------------------------------------------------------- #
# 1-bis. Build a MySQL-Shell URI from your .env variables
# --------------------------------------------------------------------------- #
def _mysqlx_uri_from_env() -> str:
    host   = os.getenv("DB_HOST")
    user   = os.getenv("DB_USER")
    passwd = os.getenv("DB_PASS")
    port   = "3306"

    if not all([host, user, passwd]):
        raise EnvironmentError(
            "DB_HOST, DB_USER and DB_PASS must be defined in your .env "
            "to build the MySQL-Shell connection URI."
        )
    return f"mysqlx://{user}:{passwd}@{host}:{port}"


# --------------------------------------------------------------------------- #
# 2.  Import implementation details
# --------------------------------------------------------------------------- #

def _import_with_embedded_shell(
    csv_path: str, *, schema: str, table: str, col_types: Mapping[str, str],
    sql_mode_lax: str, skip_rows: int, duplicate_mode: str, threads: int,
    retries: int,
) -> None:
    """The original import logic, for when running inside mysqlsh."""
    uri = _mysqlx_uri_from_env()
    sess = mysqlsh.mysql.get_session(uri)
    try:
        sess.run_sql(f"SET @@session.sql_mode = '{sql_mode_lax}';")
        uservars = [f"@{c}" for c in col_types]
        decode   = {col: f"NULLIF(@{col}, '')" for col in col_types}
        import_cfg = {
            "schema": schema, "table": table, "dialect": "csv-unix",
            "skipRows": skip_rows, "columns": uservars, "decodeColumns": decode,
            "onDuplicateKeyUpdate": duplicate_mode == "replace", "threads": threads,
        }
        for attempt in range(1, retries + 1):
            try:
                mysqlsh.util.import_table(str(csv_path), import_cfg)
                break
            except Exception as err:
                if attempt == retries: raise
                wait = 2 ** attempt + random.random()
                warnings.warn(f"Attempt {attempt} failed: {err}. Retrying in "
                              f"{wait:.1f}s …", RuntimeWarning)
                time.sleep(wait)
    finally:
        sess.close()

def _import_with_subprocess_shell(
    **params,
) -> None:
    """Import logic that invokes `mysqlsh` as a subprocess."""
    if not shutil.which("mysqlsh"):
        raise RuntimeError(
            "The 'mysqlsh' command was not found in your system's PATH. "
            "Please install MySQL Shell 8+ and ensure it is accessible."
        )

    importer_script = textwrap.dedent("""
        import json, sys, time, random, os
        try:
            import mysqlsh
        except ModuleNotFoundError:
            print("FATAL: This script must be run inside the mysqlsh environment.", file=sys.stderr)
            sys.exit(1)

        def _mysqlx_uri_from_env() -> str:
            host, user, passwd = os.getenv("DB_HOST"), os.getenv("DB_USER"), os.getenv("DB_PASS")
            port = os.getenv("DB_PORT", "33060")
            if not all([host, user, passwd]):
                raise EnvironmentError("DB_HOST, DB_USER, DB_PASS env vars are required.")
            return f"mysqlx://{user}:{passwd}@{host}:{port}"

        try:
            config = json.load(sys.stdin)
            uri = _mysqlx_uri_from_env()
            sess = mysqlsh.mysql.get_session(uri)
        except Exception as e:
            print(f"FATAL: Subprocess failed during setup: {e}", file=sys.stderr)
            sys.exit(1)

        try:
            sess.run_sql(f"SET @@session.sql_mode = '{config['sql_mode_lax']}';")
            uservars = [f"@{c}" for c in config['col_types']]
            decode = {col: f"NULLIF(@{col}, '')" for col in config['col_types']}
            import_cfg = {
                "schema": config['schema'], "table": config['table'], "dialect": "csv-unix",
                "skipRows": config['skip_rows'], "columns": uservars, "decodeColumns": decode,
                "onDuplicateKeyUpdate": config['duplicate_mode'] == "replace", "threads": config['threads'],
            }
            for attempt in range(1, config['retries'] + 1):
                try:
                    mysqlsh.util.import_table(str(config['csv_path']), import_cfg)
                    if config.get('show_progress', True):
                        print("Import successful (via mysqlsh subprocess).")
                    sys.exit(0)
                except Exception as err:
                    if attempt == config['retries']:
                        print(f"FATAL: Final import attempt failed: {err}", file=sys.stderr)
                        sys.exit(1)
                    wait = 2 ** attempt + random.random()
                    print(f"Warning: Attempt {attempt} failed: {err}. Retrying in {wait:.1f}s ...", file=sys.stderr)
                    time.sleep(wait)
        finally:
            sess.close()
    """)

    params_json = json.dumps(params)
    process = subprocess.run(
        ["mysqlsh", "--py", "-e", importer_script],
        input=params_json, text=True, capture_output=True, encoding="utf-8", check=False
    )

    if process.returncode != 0:
        error_output = (f"--- STDOUT ---\n{process.stdout}\n" f"--- STDERR ---\n{process.stderr}").strip()
        raise RuntimeError(
            f"MySQL Shell subprocess failed with exit code {process.returncode}.\n"
            f"{error_output}"
        )
    if params.get('show_progress', True) and process.stdout:
        print(process.stdout.strip())


# --------------------------------------------------------------------------- #
# 3.  Public entry point
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
    skip_rows: int = 1,                       # header
    threads: int = 8,
) -> None:
    """
    High-level wrapper around MySQL-Shell util.import_table().

    This function can run in two modes:
    1. Inside a `mysqlsh --py` session, where it uses the available `mysqlsh`
       module directly.
    2. As a standard Python script (`python load_csv.py`), where it will
       invoke the `mysqlsh` command-line tool as a subprocess. The tool
       must be in the system's PATH.

    Parameters
    ----------
    csv_path : local path to the CSV file to ingest
    table    : destination table name (will be auto-created if missing)
    schema   : destination schema; defaults to DB_NAME in .env
    … plus various tuning knobs for schema inference and import performance.
    """
    csv_path_obj = Path(csv_path).expanduser().resolve()
    if not csv_path_obj.is_file():
        raise FileNotFoundError(csv_path_obj)

    # A.  Infer schema if the caller didn't provide one explicitly
    if col_types is None:
        if show_progress: print(f"Scanning first {sample_rows:,} rows to infer schema…")
        col_types = _infer_schema(
            csv_path_obj, sample_rows, dtype_overrides, parse_dates
        )
        if show_progress:
            print("Inferred schema:")
            for col, typ in col_types.items():
                print(f"  {col:<20} {typ}")

    schema = schema or os.getenv("DB_NAME") or "public"

    # B.  Create table if it doesn’t exist
    create_sql = _render_create_sql(schema, table, col_types,
                                    primary_keys, duplicate_mode)
    import sqlalchemy as sa
    engine_url = sa.engine.url.URL.create(
        drivername="mysql+mysqldb", username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"), host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "3306"), database=schema,
    )
    with sa.create_engine(engine_url).begin() as conn:
        conn.execute(sa.text(create_sql))

    # C.  Use MySQL-Shell for the heavy lifting
    import_params = {
        "csv_path": str(csv_path_obj), "schema": schema, "table": table,
        "col_types": col_types, "sql_mode_lax": sql_mode_lax,
        "skip_rows": skip_rows, "duplicate_mode": duplicate_mode,
        "threads": threads, "retries": retries, "show_progress": show_progress,
    }

    if mysqlsh is not None:
        if show_progress: print("Running import using the embedded MySQL Shell Python environment.")
        _import_with_embedded_shell(**import_params)
    else:
        if show_progress: print("MySQL Shell module not found. Running import via `mysqlsh` subprocess.")
        _import_with_subprocess_shell(**import_params)

    if show_progress:
        print(f"✅  Import finished for {csv_path_obj.name} into {schema}.{table}")