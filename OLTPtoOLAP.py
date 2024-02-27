import pandas as pd
import numpy as np
import pyodbc as odbc
from sqlalchemy import create_engine

from Constants import CONNECTION_URL, SRC_SCHEMA, TGT_SCHEMA
from Mapper import oltp_to_olap_mapping

# Create a connection to the OLTP database
engine = create_engine(CONNECTION_URL)

# Read the data from the OLTP database by obtaining the connection
with engine.connect() as conn:
    # Mapping the OLTP tables to OLAP tables
    '''
    OLTP: Products(ProductID, ProductName, UnitPrice, Discontinued)
    OLAP: Product_dim(ProductKey, ProductID, ProductName, UnitPrice, Discontinued)
    One to one mapping of columns from OLTP to OLAP
    ''' 
    oltp_to_olap_mapping(
        'Products', 
        'Product_dim', 
        {'ProductID': 'ProductID', 'ProductName': 'ProductName', 'UnitPrice': 'UnitPrice', 'Discontinued': 'Discontinued'},
        conn
    )

