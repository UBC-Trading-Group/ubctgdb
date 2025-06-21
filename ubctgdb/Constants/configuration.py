from dotenv import load_dotenv
import os

# Load environment variables from .env file (DB_HOST, DB_NAME, DB_USER, DB_PASS)
load_dotenv()
class config:
    '''
    load all configuration values here
    '''
    driver_name = os.getenv("DB_DRIVER_NAME")
    username=os.getenv("DB_USER")
    password=os.getenv("DB_PASS")
    host=os.getenv("DB_HOST")
    database=os.getenv("DB_NAME")
