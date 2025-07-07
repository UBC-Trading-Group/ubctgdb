from __future__ import annotations

import hashlib
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import diskcache as dc
import pandas as pd
import sqlalchemy as sa
from dotenv import find_dotenv, load_dotenv

# ── env ───────────────────────────────────────────────────────────────────
load_dotenv(find_dotenv(usecwd=True), override=False)

# ── public constants ─────────────────────────────────────────────────────
NULL_TOKEN: str = r"\N"                     # recognised by MySQL *and* PyArrow
NULL_MARKERS: set[str] = {"", "na", "n/a", "nan", "null"}
PROGRESS_EVERY: int = 500_000               # rows between progress prints

# ── small on-disk dataframe cache ────────────────────────────────────────
_CACHE = dc.Cache(Path.home() / ".db_cache")

# ── SQL helpers ──────────────────────────────────────────────────────────
def q(ident: str) -> str:
    """Back-tick-quote a MySQL identifier (blocks stray back-ticks)."""
    if "`" in ident:
        raise ValueError("back-tick in identifier")
    return f"`{ident}`"


def sqlalchemy_engine(
    *,
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
) -> sa.Engine:
    """
    Build a plain SQLAlchemy *Engine* using either explicit parameters
    or `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS` from the environment.
    """
    url = sa.engine.url.URL.create(
        drivername="mysql+mysqldb",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=host or os.getenv("DB_HOST"),
        port=port or int(os.getenv("DB_PORT", "3306")),
        database=database,
    )
    return sa.create_engine(
        url,
        pool_size=5,
        pool_recycle=1800,
        pool_pre_ping=True,
    )


@contextmanager
def _engine_ctx(database: str | None = None) -> Iterator[sa.Engine]:
    eng = sqlalchemy_engine(database=database)
    try:
        yield eng
    finally:
        eng.dispose()

# ── dataframe query helper with 24-h cache ───────────────────────────────
def _cache_key(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def run_sql(
    sql: str,
    *,
    refresh: bool = False,
    chunksize: int | None = 50_000,
    database: str | None = None,
) -> pd.DataFrame:
    """
    Execute *sql* and return a DataFrame, streaming in *chunksize* pieces
    to limit memory spikes.  Results persist in an on-disk cache for 24 h.
    """
    key = _cache_key(sql)
    if not refresh and key in _CACHE:
        return _CACHE[key]

    with _engine_ctx(database) as eng:
        if chunksize:
            it = pd.read_sql_query(sql, eng, chunksize=chunksize)
            df = pd.concat(list(it), ignore_index=True)
        else:
            df = pd.read_sql_query(sql, eng)

    _CACHE.set(key, df, expire=24 * 3600)
    return df