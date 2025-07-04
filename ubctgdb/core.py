import os
import hashlib
from contextlib import contextmanager

import pandas as pd
import sqlalchemy as sa
import diskcache as dc
from dotenv import load_dotenv

# Load environment variables from .env file (DB_HOST, DB_NAME, DB_USER, DB_PASS)
load_dotenv()

# Initialize a local on-disk cache
_CACHE = dc.Cache(os.path.expanduser("~/.db_cache"))

@contextmanager
def _engine():
    """
    Creates a SQLAlchemy engine for connecting to the MySQL database.
    """
    url = sa.engine.url.URL.create(
        drivername="mysql+mysqldb",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
    )
    engine = sa.create_engine(
        url,
        pool_size=5,
        pool_recycle=1800,
        pool_pre_ping=True,
    )
    try:
        yield engine
    finally:
        engine.dispose()


def _generate_cache_key(sql: str) -> str:
    """
    Generates a unique cache key based on the SQL query string.
    """
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def run_sql(sql: str, *, refresh: bool = False, chunksize: int = 50000) -> pd.DataFrame:
    """
    Executes a SQL query against the MySQL database and returns the result as a pandas DataFrame.
    """
    key = _generate_cache_key(sql)

    # Return cached DataFrame if available
    if not refresh and key in _CACHE:
        return _CACHE[key]

    # Execute query
    with _engine() as engine:
        if chunksize:
            # Read in iterable chunks, then combine into one DataFrame
            iterator = pd.read_sql_query(sql, engine, chunksize=chunksize)
            df = pd.concat(list(iterator), ignore_index=True)
        else:
            # Load entire result in one shot
            df = pd.read_sql_query(sql, engine)

    # Store in cache for 24 hours
    _CACHE.set(key, df, expire=24 * 3600)
    return df

def get_connection_endpoint() -> str:
    """
    Returns the connection endpoint for the MySQL database.
    """
    return f"mysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"