from sqlalchemy.engine import URL
import logging
import os, json

# Define the connection strings and other parameters
OLTP_SCHEMA = 'dbo'
LND_SCHEMA = 'DW_Landing'
STG_SCHEMA = 'DW_Staging'
DW_SCHEMA = 'DW'
DB_NAME = 'Capstone'

REFRESH_TABLES = True
REFRESH_PROCEDURES = True
DIM_TABLES = ['Product_Dim', 'Supplier_Dim', 'Categories_Dim', 'Calendar_Dim', 'Customer_Dim', 'Employee_Dim']
FACT_TABLES = ['CustomerEmployee_Fact', 'ProductInStock_Fact']
MATCHING_CONDITIONS = {
    'Product_Dim': 'TGT.ProductID=SRC.ProductID',
    'Supplier_Dim': 'TGT.SupplierID=SRC.SupplierID',
    'Categories_Dim': 'TGT.CategoryID=SRC.CategoryID',
    'Calendar_Dim': 'TGT.FullDate=SRC.FullDate',
    'Customer_Dim': 'TGT.CustomerID=SRC.CustomerID',
    'Employee_Dim': 'TGT.EmployeeID=SRC.EmployeeID',
}
BRIDGE_TABLES = []

logger = None

CONNECTION_STRING = 'DRIVER={ODBC Driver 17 for SQL Server};''SERVER=IN3540133W1;'f'DATABASE={DB_NAME};''TRUSTED_CONNECTION=yes;'
CONNECTION_URL = URL.create('mssql+pyodbc', query={'odbc_connect': CONNECTION_STRING})

try:
    file_path = os.path.join(os.getcwd(), 'config.json')
    with open(file_path, 'r') as file:
        config = json.load(file)
        db = config['database']
        CONNECTION_STRING = f"DRIVER={{{db['driver']}}};"f"SERVER={db['server']};"f"DATABASE={db['databaseName']};""TRUSTED_CONNECTION=yes;"
        CONNECTION_URL = URL.create('mssql+pyodbc', query={'odbc_connect': CONNECTION_STRING})

        OLTP_SCHEMA = db['OLTP_SCHEMA']
        LND_SCHEMA = db['LND_SCHEMA']
        STG_SCHEMA = db['STG_SCHEMA']
        DW_SCHEMA = db['DW_SCHEMA']
        DB_NAME = db['databaseName']
        DIM_TABLES = db['dimensionTables']
        FACT_TABLES = db['factTables']
        MATCHING_CONDITIONS = db['matchingConditions']
        BRIDGE_TABLES = db['bridgeTables']

        REFRESH_TABLES = config['refreshTables']
        REFRESH_PROCEDURES = config['refreshProcedures']

        logging.basicConfig(filename=config['logging']['filename'],
            filemode='w', 
            format='[%(filename)s:%(lineno)s - %(funcName)30s()] - %(levelname)s - %(asctime)s - %(message)s', 
            level=logging.DEBUG,
            datefmt='%d-%m-%Y %H:%M:%S'
        )
        logger = logging.getLogger(config['logging']['name'])
except Exception as e:
    print(f'Error in parsing the config file: {e}')
    print(f'Using the default values for the config parameters.')
    