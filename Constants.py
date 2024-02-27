from sqlalchemy.engine import URL

# Define the connection strings and other parameters
SRC_SCHEMA = 'dbo'
TGT_SCHEMA = 'DW_Landing'
DB_NAME = 'Capstone'


CONNECTION_STRING = 'DRIVER={SQL Server};''SERVER=IN3540133W1;'f'DATABASE={DB_NAME};''TRUSTED_CONNECTION=yes;'
CONNECTION_URL = URL.create('mssql+pyodbc', query={'odbc_connect': CONNECTION_STRING})