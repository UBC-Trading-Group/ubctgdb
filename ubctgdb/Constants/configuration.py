from dotenv import load_dotenv
import os

# Load environment variables from .env file (DB_HOST, DB_NAME, DB_USER, DB_PASS)
load_dotenv()

class Config:  
    '''
    load all configuration values here
    '''
    _driver_name = os.getenv("DB_DRIVER_NAME")
    _username=os.getenv("DB_USER")
    _password=os.getenv("DB_PASS")
    _host=os.getenv("DB_HOST")
    _database=os.getenv("DB_NAME")

    @classmethod
    def refresh_config(cls):
        load_dotenv(override=True)
        cls._driver_name = os.getenv("DB_DRIVER_NAME")
        cls._username=os.getenv("DB_USER")
        cls._password=os.getenv("DB_PASS")
        cls._host=os.getenv("DB_HOST")
        cls._database=os.getenv("DB_NAME")

