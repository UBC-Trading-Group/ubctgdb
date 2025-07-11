import sqlalchemy as sa
from ubctgdb.Constants.configuration import Config
from sqlalchemy.orm import sessionmaker
from functools import wraps

def singleton(class_):
    '''
    singleton class decorator enforcing one connection
    '''
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance

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

@singleton
class DBConn:
    def __init__(self):
        self.engine = None
        self.sessionmaker = None

    def connect(self):
        '''
        connects to the database
        '''
        try:
            if not self.sessionmaker:
                self.engine = sa.create_engine(build_conn_url(), echo=False)
                self.sessionmaker = sessionmaker(bind=self.engine)
        except Exception:
            raise
        return self

    @require_connection
    def get_session(self):
        return self.sessionmaker

    @require_connection
    def execute(self, query, params=None):
        with self.sessionmaker() as session:
            result = session.execute(query, params or {})
            return result.fetchall()