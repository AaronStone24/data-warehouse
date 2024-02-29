import pandas as pd
import numpy as np

from Config import OLTP_SCHEMA, LND_SCHEMA, STG_SCHEMA, DW_SCHEMA, logger
from Exceptions import TableExistsError, MappingError

'''
    Function to map the OLTP tables to OLAP tables
    :param oltp_table_name: Name of the OLTP table
    :param olap_table_name: Name of the OLAP table
    :param mapping: Dictionary with the mapping of columns from OLTP to OLAP
    :param conn: Connection to the OLTP database
'''
def oltp_to_olap_mapping(oltp_table_name, olap_table_name, mapping: dict, conn):
    try:
        logger.info(f'Starting mapping of {OLTP_SCHEMA}.{oltp_table_name} to {LND_SCHEMA}.{olap_table_name}.')
        oltp_columns, olap_columns = list(mapping.keys()), list(mapping.values())

        sql_query = f'SELECT {','.join(oltp_columns)} FROM {OLTP_SCHEMA}.{oltp_table_name}'
        if olap_table_name == 'Calendar_Dim':
            sql_query = f'SELECT DISTINCT {oltp_columns[0]} FROM {OLTP_SCHEMA}.{oltp_table_name}'
        logger.info(f'Executing the query to select data from oltp table: {sql_query}')
        
        oltp_df = pd.read_sql_query(sql_query, conn)
        print(oltp_df.head())
        logger.info(f'Read the data from the OLTP database: {OLTP_SCHEMA}.{oltp_table_name}.')

        # Replace the missing values with NaN
        oltp_df.replace('', np.NaN, inplace=True)
        logger.info(f'Replaced the missing values with NaN in the dataframe.')

        # Rename the columns
        oltp_df = oltp_df.rename(columns=mapping)
        logger.info(f'Renamed the columns in the dataframe from oltp to olap ones.')

        # Special case for Calendar_Dim
        if olap_table_name == 'Calendar_Dim':
            oltp_df['DayofTheWeek'] = oltp_df['FullDate'].dt.day_name()
            oltp_df['DayType'] = np.where(oltp_df['DayofTheWeek'].isin(['Saturday', 'Sunday']), 'Weekend', 'Weekday')
            oltp_df['DayofTheMonth'] = oltp_df['FullDate'].dt.day
            oltp_df['Month'] = oltp_df['FullDate'].dt.month
            oltp_df['Quarter'] = oltp_df['FullDate'].dt.quarter
            oltp_df['Year'] = oltp_df['FullDate'].dt.year
            logger.info(f'Added the extra columns to the dataframe for Calendar_Dim dimension table.')

        # Add the SourceTable column to the DataFrame
        oltp_df['SourceTable'] = f'{OLTP_SCHEMA}.{oltp_table_name}'
        logger.info(f'Added the SourceTable column to the DataFrame.')

        # Add the loadTimeDate column to the DataFrame
        oltp_df['loadTimeDate'] = pd.to_datetime('now')
        logger.info(f'Added the loadTimeDate column to the DataFrame.')

        # Write the data to the OLAP database
        try:
            oltp_df.to_sql(olap_table_name, conn, schema=LND_SCHEMA, if_exists='append', index=False)
            logger.info(f'Wrote the data to the {LND_SCHEMA}.{olap_table_name} OLAP database.')
        except ValueError as e:
            raise TableExistsError(f"Table '{LND_SCHEMA}.{olap_table_name}' already exists in the OLAP database.")
        except Exception as e:
            raise e

        conn.commit()
        logger.info(f'Committed the changes to the {LND_SCHEMA}.{olap_table_name} in OLAP database.')
    
    except TableExistsError as e:
        logger.warning(e)
        # raise e

    except Exception as e:
        logger.error(f'Error in mapping {OLTP_SCHEMA}.{oltp_table_name} to {LND_SCHEMA}.{olap_table_name}: {e}')
        raise MappingError(oltp_table_name, olap_table_name, f'Error in mapping {OLTP_SCHEMA}.{oltp_table_name} to {LND_SCHEMA}.{olap_table_name}: {e}')

def customerEmployee_Fact_mapper(conn):
    try:
        sql_query = '''SELECT CSD.CustomerKey, ED.EmployeeKey, CD.CalendarKey, 
            CEB.OrderID, CEB.Sales
            FROM {1}.CustomerEmployee_Bridge CEB
            JOIN {0}.Customer_Dim CSD
            ON CEB.CustomerID = CSD.CustomerID AND CSD.endTimeDate IS NULL
            JOIN {0}.Employee_Dim ED
            ON CEB.EmployeeID = ED.EmployeeID AND ED.endTimeDate IS NULL
            JOIN {0}.Calendar_Dim CD
            ON CEB.OrderDate = CD.FullDate AND CD.endTimeDate IS NULL'''.format(DW_SCHEMA, STG_SCHEMA)
        
        logger.info(f'Executing the query to select data from {STG_SCHEMA}.CustomerEmployee_Bridge: {sql_query}')
        
        df = pd.read_sql_query(sql_query, conn)
        print(df.head())
        logger.info(f'Read the data from {STG_SCHEMA}.CustomerEmployee_Bridge')

        # Replace the missing values with NaN
        df.replace('', np.NaN, inplace=True)
        logger.info(f'Replaced the missing values with NaN in the dataframe.')

        # Add the SourceTable column to the DataFrame
        df['SourceTable'] = f'{STG_SCHEMA}.CustomerEmployee_Bridge'
        logger.info(f'Added the SourceTable column to the DataFrame.')

        # Add the loadTimeDate column to the DataFrame
        df['loadTimeDate'] = pd.to_datetime('now')
        logger.info(f'Added the loadTimeDate column to the DataFrame.')

        # Write the data to the OLAP database
        df.to_sql('CustomerEmployee_Fact', conn, schema=DW_SCHEMA, if_exists='append', index=False)
        logger.info(f'Written the data to the {DW_SCHEMA}.CustomerEmployee_Fact table.')
        
        conn.commit()
        logger.info(f'Committed the changes to the {DW_SCHEMA}.CustomerEmployee_Fact table.')
    
    except Exception as e:
        logger.error(f'Error in creating {DW_SCHEMA}.CustomerEmployee_Fact: {e}')

def productInStock_Fact_mapper(conn):
    try:
        sql_query = '''SELECT CD.CalendarKey, PD.ProductKey, CTD.CategoriesKey, SD.SupplierKey,
        PIB.UnitsInStock, PIB.UnitsOnOrder, PIB.ReorderLevel, PIB.TotalQuantity, PIB.OrderId
        FROM {0}.ProductInStock_Bridge PIB
        JOIN {1}.Calendar_Dim CD
        ON PIB.OrderDate = CD.FullDate AND CD.endTimeDate IS NULL
        JOIN {1}.Product_Dim PD
        ON PIB.ProductID = PD.ProductID AND PD.endTimeDate IS NULL
        JOIN {1}.Categories_Dim CTD
        ON PIB.CategoryID = CTD.CategoryID AND CTD.endTimeDate IS NULL
        JOIN {1}.Supplier_Dim SD
        ON PIB.SupplierID = SD.SupplierID AND SD.endTimeDate IS NULL'''.format(STG_SCHEMA, DW_SCHEMA)

        logger.info(f'Executing the query to select data from {STG_SCHEMA}.ProductInStock_Bridge: {sql_query}')
        
        df = pd.read_sql_query(sql_query, conn)
        print(df.head())
        logger.info(f'Read the data from {STG_SCHEMA}.ProductInStock_Bridge')

        # Replace the missing values with NaN
        df.replace('', np.NaN, inplace=True)
        logger.info(f'Replaced the missing values with NaN in the dataframe.')

        # Add the SourceTable column to the DataFrame
        df['SourceTable'] = f'{STG_SCHEMA}.ProductInStock_Bridge'
        logger.info(f'Added the SourceTable column to the DataFrame.')

        # Add the loadTimeDate column to the DataFrame
        df['loadTimeDate'] = pd.to_datetime('now')
        logger.info(f'Added the loadTimeDate column to the DataFrame.')

        # Write the data to the OLAP database
        df.to_sql('ProductInStock_Fact', conn, schema=DW_SCHEMA, if_exists='append', index=False)
        logger.info(f'Written the data to the {DW_SCHEMA}.ProductInStock_Bridge table.')
        
        conn.commit()
        logger.info(f'Committed the changes to the {DW_SCHEMA}.ProductInStock_Fact table.')
    
    except Exception as e:
        logger.error(f'Error in creating {DW_SCHEMA}.ProductInStock_Fact: {e}')

def customerEmployee_Bridge_mapper(conn):
    try:
        sql_query = '''SELECT OD.CustomerID, OD.EmployeeID, OD.OrderDate, OD.OrderID, 
        SUM(UnitPrice * (1 - Discount) * Quantity) AS Sales
        FROM {0}.Orders OD
        JOIN {0}.[Order Details] ODT
        ON OD.OrderID = ODT.OrderID
        GROUP BY OD.CustomerID, OD.EmployeeID, OD.OrderDate, OD.OrderID'''.format(OLTP_SCHEMA)
        logger.info(f'Executing the query to select data from oltp tables: {sql_query}')

        df = pd.read_sql_query(sql_query, conn)
        print(df.head())
        logger.info(f'Read the data from the OLTP database: {OLTP_SCHEMA}.Orders, {OLTP_SCHEMA}.OrderDetails.')

        # Replace the missing values with NaN
        df.replace('', np.NaN, inplace=True)
        logger.info(f'Replaced the missing values with NaN in the dataframe.')

        # Add the SourceTable column to the DataFrame
        df['SourceTable'] = f'{OLTP_SCHEMA}.Orders, {OLTP_SCHEMA}.[Order Details]'
        logger.info(f'Added the SourceTable column to the DataFrame.')

        # Add the loadTimeDate column to the DataFrame
        df['loadTimeDate'] = pd.to_datetime('now')
        logger.info(f'Added the loadTimeDate column to the DataFrame.')

        # Write the data to the OLAP database
        df.to_sql('CustomerEmployee_Bridge', conn, schema=LND_SCHEMA, if_exists='append', index=False)
        logger.info(f'Written the data to the {LND_SCHEMA}.CustomerEmployee_Bridge OLAP database.')

        conn.commit()
        logger.info(f'Committed the changes to the {LND_SCHEMA}.CustomerEmployee_Bridge OLAP database.')
    except Exception as e:
        logger.error(f'Error in mapping CustomerEmployee_Bridge: {e}')

def productInStock_Bridge_mapper(conn):
    try:
        sql_query = '''SELECT PDT.ProductID, PDT.CategoryID, PDT.SupplierID, UnitsInStock, UnitsOnOrder, ReorderLevel,
        OD.OrderDate,
        SUM(Quantity) OVER (PARTITION BY ODT.ProductID) AS TotalQuantity,
        OD.OrderID
        FROM {0}.Orders OD
        JOIN {0}.[Order Details] ODT ON OD.OrderID = ODT.OrderID
        JOIN {0}.Products PDT ON ODT.ProductID = PDT.ProductID'''.format(OLTP_SCHEMA)
        logger.info(f'Executing the query to select data from oltp tables: {sql_query}')

        df = pd.read_sql_query(sql_query, conn)
        print(df.head())
        logger.info(f'Read the data from the OLTP database: {OLTP_SCHEMA}.Orders, {OLTP_SCHEMA}.[Order Details], {OLTP_SCHEMA}.Products.')

        # Replace the missing values with NaN
        df.replace('', np.NaN, inplace=True)
        logger.info(f'Replaced the missing values with NaN in the dataframe.')

        # Add the SourceTable column to the DataFrame
        df['SourceTable'] = f'{OLTP_SCHEMA}.Orders, {OLTP_SCHEMA}.[Order Details], {OLTP_SCHEMA}.Products'
        logger.info(f'Added the SourceTable column to the DataFrame.')

        # Add the loadTimeDate column to the DataFrame
        df['loadTimeDate'] = pd.to_datetime('now')
        logger.info(f'Added the loadTimeDate column to the DataFrame.')

        # Write the data to the OLAP database
        df.to_sql('ProductInStock_Bridge', conn, schema=LND_SCHEMA, if_exists='append', index=False)
        logger.info(f'Written the data to the {LND_SCHEMA}.ProductInStock_Bridge OLAP database.')

        conn.commit()
        logger.info(f'Committed the changes to the {LND_SCHEMA}.ProductInStock_Bridge OLAP database.')
    except Exception as e:
        logger.error(f'Error in mapping ProductInStock_Bridge: {e}')


# OLD Fact table creation code for testing purposes
# def customerEmployee_Fact_mapper(conn):
#     try:
#         sql_query = '''SELECT CustomerKey, EmployeeKey, CalendarKey, OD.OrderID,
#             SUM(UnitPrice * (1 - Discount) * Quantity) AS Sales
#             FROM {0}.Orders OD
#             JOIN {1}.Customer_Dim CD
#             ON OD.CustomerID = CD.CustomerID
#             JOIN {1}.Employee_Dim ED
#             ON OD.EmployeeID = ED.EmployeeID
#             JOIN {1}.Calendar_Dim CLD
#             ON OD.OrderDate = CLD.FullDate
#             JOIN {0}.[Order Details] ODT
#             ON OD.OrderID = ODT.OrderID
#             GROUP BY CustomerKey, EmployeeKey, CalendarKey, OD.OrderID'''.format(OLTP_SCHEMA, LND_SCHEMA)
#         logger.info(f'Executing the query to select data from oltp tables: {sql_query}')
        
#         df = pd.read_sql_query(sql_query, conn)
#         print(df.head())
#         logger.info(f'Read the data from the OLTP database: Orders, OrderDetails.')

#         # Replace the missing values with NaN
#         df.replace('', np.NaN, inplace=True)
#         logger.info(f'Replaced the missing values with NaN in the dataframe.')

#         # Add the SourceTable column to the DataFrame
#         df['SourceTable'] = f'{OLTP_SCHEMA}.Orders, {OLTP_SCHEMA}.OrderDetails'
#         logger.info(f'Added the SourceTable column to the DataFrame.')

#         # Add the loadTimeDate column to the DataFrame
#         df['loadTimeDate'] = pd.to_datetime('now')
#         logger.info(f'Added the loadTimeDate column to the DataFrame.')

#         # Write the data to the OLAP database
#         df.to_sql('CustomerEmployee_Fact', conn, schema=LND_SCHEMA, if_exists='append', index=False)
#         logger.info(f'Written the data to the CustomerEmployee_Fact OLAP database.')
        
#         conn.commit()
#         logger.info(f'Committed the changes to the CustomerEmployee_Fact OLAP database.')
    
#     except Exception as e:
#         logger.error(f'Error in mapping CustomerEmployee_Fact: {e}')

# def productInStock_Fact_mapper(conn):
#     try:
#         sql_query = '''SELECT CalendarKey, ProductKey, CategoriesKey, SupplierKey, UnitsInStock, UnitsOnOrder, ReorderLevel,
#         SUM(Quantity) OVER (PARTITION BY ODT.ProductID) AS TotalQuantity,
#         OD.OrderID
#         FROM {0}.Orders OD
#         JOIN {0}.[Order Details] ODT ON OD.OrderID = ODT.OrderID
#         JOIN {0}.Products PDT ON ODT.ProductID = PDT.ProductID
#         JOIN {1}.Calendar_Dim CLD ON OD.OrderDate = CLD.FullDate
#         JOIN {1}.Product_Dim PD ON ODT.ProductID = PD.ProductID
#         JOIN {1}.Categories_Dim CD ON PDT.CategoryID = CD.CategoryID
#         JOIN {1}.Supplier_Dim SD ON PDT.SupplierID = SD.SupplierID'''.format(OLTP_SCHEMA, LND_SCHEMA)
#         logger.info(f'Executing the query to select data from oltp tables: {sql_query}')

#         df = pd.read_sql_query(sql_query, conn)
#         print(df.head())
#         logger.info(f'Read the data from the OLTP database: Orders, OrderDetails, Products.')

#         # Replace the missing values with NaN
#         df.replace('', np.NaN, inplace=True)
#         logger.info(f'Replaced the missing values with NaN in the dataframe.')

#         # Add the SourceTable column to the DataFrame
#         df['SourceTable'] = f'{OLTP_SCHEMA}.Orders, {OLTP_SCHEMA}.OrderDetails, {OLTP_SCHEMA}.Products'
#         logger.info(f'Added the SourceTable column to the DataFrame.')

#         # Add the loadTimeDate column to the DataFrame
#         df['loadTimeDate'] = pd.to_datetime('now')
#         logger.info(f'Added the loadTimeDate column to the DataFrame.')

#         # Write the data to the OLAP database
#         df.to_sql('ProductInStock_Fact', conn, schema=LND_SCHEMA, if_exists='append', index=False)
#         logger.info(f'Written the data to the ProductInStock_Fact OLAP database.')

#         conn.commit()
#         logger.info(f'Committed the changes to the ProductInStock_Fact OLAP database.')
#     except Exception as e:
#         logger.error(f'Error in mapping ProductInStock_Fact: {e}')
