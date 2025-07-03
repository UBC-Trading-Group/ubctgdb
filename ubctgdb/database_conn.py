import sqlalchemy as sa
from ubctgdb.Constants.configuration import Config
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from functools import wraps

# def singleton(class_):
#     '''
#     singleton class decorator enforcing one connection
#     '''
#     instances = {}
#     def getinstance(*args, **kwargs):
#         if class_ not in instances:
#             instances[class_] = class_(*args, **kwargs)
#         return instances[class_]
#     return getinstance

def require_connection(func):
    '''
    connection enforcer decorator used on ensuing functions
    '''
    @wraps(func)
    def wrapper(cls, *args, **kwargs):
        if not getattr(cls, "_sessionmaker", None):
            raise ConnectionError("Database not connected cannot proceed.")
        return func(cls, *args, **kwargs)
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

class DbConn:
    _engine = None
    _sessionmaker=None

    @classmethod
    async def connect(cls):
        '''
        connects to the database
        '''
        if cls._sessionmaker:
            return 
        
        try:
            print(f"the url is {build_conn_url()}")
            cls._engine = create_async_engine(build_conn_url(), echo=False)
            cls._sessionmaker = async_sessionmaker(bind=DbConn._engine, expire_on_commit=False)
        except Exception:
            raise
        return cls
    
    @classmethod
    @require_connection
    async def execute(cls, query, params=None):
        async with cls._sessionmaker() as session:
            result = await session.execute(query, params or {})
            return result.fetchall()
    