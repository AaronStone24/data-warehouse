from sqlalchemy.engine import URL
import logging

# Define the connection strings and other parameters
OLTP_SCHEMA = 'dbo'
LND_SCHEMA = 'DW_Landing'
STG_SCHEMA = 'DW_Staging'
DW_SCHEMA = 'DW'
DB_NAME = 'Capstone'

DIM_TABLES = ['Product_Dim', 'Supplier_Dim', 'Categories_Dim', 'Calendar_Dim', 'Customer_Dim', 'Employee_Dim']
FACT_TABLES = ['CustomerEmployee_Fact', 'ProductInStock_Fact']
MATCHING_CONDITIONS = {
    'Product_Dim': 'TGT.ProductID=SRC.ProductID',
    'Supplier_Dim': 'TGT.SupplierID=SRC.SupplierID',
    'Categories_Dim': 'TGT.CategoryID=SRC.CategoryID',
    'Calendar_Dim': 'TGT.FullDate=SRC.FullDate', # TODO: Review this matching condition
    'Customer_Dim': 'TGT.CustomerID=SRC.CustomerID',
    'Employee_Dim': 'TGT.EmployeeID=SRC.EmployeeID',
}
BRIDGE_TABLES = []

CONNECTION_STRING = 'DRIVER={ODBC Driver 17 for SQL Server};''SERVER=IN3540133W1;'f'DATABASE={DB_NAME};''TRUSTED_CONNECTION=yes;'
CONNECTION_URL = URL.create('mssql+pyodbc', query={'odbc_connect': CONNECTION_STRING})

logging.basicConfig(filename='DW.log',
    filemode='w', 
    format='[%(filename)s:%(lineno)s - %(funcName)30s()] - %(levelname)s - %(asctime)s - %(message)s', 
    level=logging.DEBUG,
    datefmt='%d-%m-%Y %H:%M:%S'
)
logger = logging.getLogger('DW')