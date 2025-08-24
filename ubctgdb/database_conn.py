import sqlalchemy as sa
from ubctgdb.Constants.configuration import Config
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from functools import wraps
import ubctgdb.singleton as Singleton

def require_connection(func):
    '''
    connection enforcer decorator used on ensuing functions
    '''
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not getattr(self, "sessionmaker", None):
            raise ConnectionError("Database not connected cannot proceed.")
        return func(self, *args, **kwargs)
    return wrapper

def build_conn_url():
    '''
    builds connection url and refreshes configuration with appropriate values
    '''
    Config.refresh_config()
    return sa.engine.url.URL.create(
        drivername=Config._driver_name,
        username=Config._username,
        password=Config._password,
        host=Config._host,
        database=Config._database
    )

class DBConn:
    __metaclass__ = Singleton
    
    def __init__(self):
        self.engine = None
        self.sessionmaker = None

    async def connect(self):
        '''
        connects to the database
        '''
        try:
            if not self.sessionmaker:
                self.engine = create_async_engine(build_conn_url(), echo=False)
                self.sessionmaker = async_sessionmaker(bind=self.engine, expire_on_commit=False)
        except Exception:
            raise
        return self

    @require_connection
    def get_session(self):
        return self.sessionmaker

    @require_connection
    async def execute(self, query, params=None):
        async with self.sessionmaker() as session:
            result = await session.execute(query, params or {})
            rows = result.fetchall()
            return [dict(row._mapping) for row in rows]