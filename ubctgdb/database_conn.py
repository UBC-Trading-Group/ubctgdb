import sqlalchemy as sa
from ubctgdb.Constants.configuration import config


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
    def wrapper(self, *args, **kwargs):
        if not self.connection:
            raise ConnectionError("Database not connected cannot proceed.")
        return func(self, *args, **kwargs)
    return wrapper

def build_conn_url():
    return sa.engine.url.URL.create(
        drivername=config.driver_name,
        username=config.username,
        password=config.password,
        host=config.host,
        database=config.database
    )

@singleton
class DbConn:
    def __init__(self) -> None:
        self.connection = None
    
    def connect(self):
        '''
        connects to the database
        '''
        try: 
         engine = sa.create_engine(build_conn_url())
         self.connection = engine.connect()
        except Exception:
            raise
    
    @require_connection
    def close_conn(self):
        '''
        closes connection
        '''
        self.connection.close()
        print("connection has closed")
    
    @require_connection
    def get_conn(self):
        return self.connection