import hashlib, os, pandas as pd, sqlalchemy as sa, diskcache as dc
from contextlib import contextmanager
from dotenv import load_dotenv
load_dotenv(".env")

_CACHE = dc.Cache(os.path.expanduser("~/.db_cache"))

@contextmanager
def _engine():
    url = sa.engine.url.URL.create(
        "mysql+mysqldb",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
    )
    eng = sa.create_engine(url, pool_size=5, pool_recycle=1800, pool_pre_ping=True)
    try:
        yield eng
    finally:
        eng.dispose()

def _cache_key(sql: str) -> str:
    return hashlib.sha256(sql.encode()).hexdigest()

def run_sql(sql: str, *, refresh: bool = False, chunksize: int = 50_000) -> pd.DataFrame:
    key = _cache_key(sql)
    if not refresh and key in _CACHE:
        return _CACHE[key]

    with _engine() as eng:
        dfs = pd.read_sql_query(sql, eng, chunksize=chunksize)
        df = pd.concat(dfs) if isinstance(dfs, pd.io.parsers.TextFileReader) else dfs

    _CACHE.set(key, df, expire=24 * 3600)  # 24-h TTL
    return df
