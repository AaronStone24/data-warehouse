from sqlalchemy.engine import URL
import logging

# Define the connection strings and other parameters
SRC_SCHEMA = 'dbo'
TGT_SCHEMA = 'DW_Landing'
DB_NAME = 'Capstone'


CONNECTION_STRING = 'DRIVER={ODBC Driver 17 for SQL Server};''SERVER=IN3540133W1;'f'DATABASE={DB_NAME};''TRUSTED_CONNECTION=yes;'
CONNECTION_URL = URL.create('mssql+pyodbc', query={'odbc_connect': CONNECTION_STRING})

logging.basicConfig(filename='DW.log',
    filemode='w', 
    format='[%(filename)s:%(lineno)s - %(funcName)20s()] - %(levelname)s - %(asctime)s - %(message)s', 
    level=logging.DEBUG,
    datefmt='%d-%m-%Y %H:%M:%S'
)
logger = logging.getLogger('DW')