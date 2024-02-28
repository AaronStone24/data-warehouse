import pandas as pd
import numpy as np

from Constants import OLTP_SCHEMA, LND_SCHEMA, logger

'''
    Function to map the OLTP tables to OLAP tables
    :param oltp_table_name: Name of the OLTP table
    :param olap_table_name: Name of the OLAP table
    :param mapping: Dictionary with the mapping of columns from OLTP to OLAP
    :param conn: Connection to the OLTP database
'''
def oltp_to_olap_mapping(oltp_table_name, olap_table_name, mapping: dict, conn):
    try:
        logger.info(f'Starting mapping of {oltp_table_name} to {olap_table_name}.')
        oltp_columns, olap_columns = list(mapping.keys()), list(mapping.values())

        sql_query = f'SELECT {','.join(oltp_columns)} FROM {OLTP_SCHEMA}.{oltp_table_name}'
        if olap_table_name == 'Calendar_Dim':
            sql_query = f'SELECT DISTINCT {oltp_columns[0]} FROM {OLTP_SCHEMA}.{oltp_table_name}'
        logger.info(f'Executing the query to select data from oltp table: {sql_query}')
        
        oltp_df = pd.read_sql_query(sql_query, conn)
        print(oltp_df.head())
        logger.info(f'Read the data from the OLTP database: {oltp_table_name}.')

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
        oltp_df.to_sql(olap_table_name, conn, schema=LND_SCHEMA, if_exists='fail', index=False)
        logger.info(f'Written the data to the {olap_table_name} OLAP database.')

        conn.commit()
        logger.info(f'Committed the changes to the {olap_table_name} OLAP database.')
    
    except Exception as e:
        logger.error(f'Error in mapping {oltp_table_name} to {olap_table_name}: {e}')

def customerEmployee_Fact_mapper(conn):
    try:
        sql_query = '''SELECT CustomerKey, EmployeeKey, CalendarKey, OD.OrderID,
            SUM(UnitPrice * (1 - Discount) * Quantity) AS Sales
            FROM {0}.Orders OD
            JOIN {1}.Customer_Dim CD
            ON OD.CustomerID = CD.CustomerID
            JOIN {1}.Employee_Dim ED
            ON OD.EmployeeID = ED.EmployeeID
            JOIN {1}.Calendar_Dim CLD
            ON OD.OrderDate = CLD.FullDate
            JOIN {0}.[Order Details] ODT
            ON OD.OrderID = ODT.OrderID
            GROUP BY CustomerKey, EmployeeKey, CalendarKey, OD.OrderID'''.format(OLTP_SCHEMA, LND_SCHEMA)
        logger.info(f'Executing the query to select data from oltp tables: {sql_query}')
        
        df = pd.read_sql_query(sql_query, conn)
        print(df.head())
        logger.info(f'Read the data from the OLTP database: Orders, OrderDetails.')

        # Replace the missing values with NaN
        df.replace('', np.NaN, inplace=True)
        logger.info(f'Replaced the missing values with NaN in the dataframe.')

        # Add the SourceTable column to the DataFrame
        df['SourceTable'] = f'{OLTP_SCHEMA}.Orders, {OLTP_SCHEMA}.OrderDetails'
        logger.info(f'Added the SourceTable column to the DataFrame.')

        # Add the loadTimeDate column to the DataFrame
        df['loadTimeDate'] = pd.to_datetime('now')
        logger.info(f'Added the loadTimeDate column to the DataFrame.')

        # Write the data to the OLAP database
        df.to_sql('CustomerEmployee_Fact', conn, schema=LND_SCHEMA, if_exists='fail', index=False)
        logger.info(f'Written the data to the CustomerEmployee_Fact OLAP database.')
        
        conn.commit()
        logger.info(f'Committed the changes to the CustomerEmployee_Fact OLAP database.')
    
    except Exception as e:
        logger.error(f'Error in mapping CustomerEmployee_Fact: {e}')

def productInStock_Fact_mapper(conn):
    try:
        sql_query = '''SELECT CalendarKey, ProductKey, CategoriesKey, SupplierKey, UnitsInStock, UnitsOnOrder, ReorderLevel,
        SUM(Quantity) OVER (PARTITION BY ODT.ProductID) AS TotalQuantity,
        OD.OrderID
        FROM {0}.Orders OD
        JOIN {0}.[Order Details] ODT ON OD.OrderID = ODT.OrderID
        JOIN {0}.Products PDT ON ODT.ProductID = PDT.ProductID
        JOIN {1}.Calendar_Dim CLD ON OD.OrderDate = CLD.FullDate
        JOIN {1}.Product_Dim PD ON ODT.ProductID = PD.ProductID
        JOIN {1}.Categories_Dim CD ON PDT.CategoryID = CD.CategoryID
        JOIN {1}.Supplier_Dim SD ON PDT.SupplierID = SD.SupplierID'''.format(OLTP_SCHEMA, LND_SCHEMA)
        logger.info(f'Executing the query to select data from oltp tables: {sql_query}')

        df = pd.read_sql_query(sql_query, conn)
        print(df.head())
        logger.info(f'Read the data from the OLTP database: Orders, OrderDetails, Products.')

        # Replace the missing values with NaN
        df.replace('', np.NaN, inplace=True)
        logger.info(f'Replaced the missing values with NaN in the dataframe.')

        # Add the SourceTable column to the DataFrame
        df['SourceTable'] = f'{OLTP_SCHEMA}.Orders, {OLTP_SCHEMA}.OrderDetails, {OLTP_SCHEMA}.Products'
        logger.info(f'Added the SourceTable column to the DataFrame.')

        # Add the loadTimeDate column to the DataFrame
        df['loadTimeDate'] = pd.to_datetime('now')
        logger.info(f'Added the loadTimeDate column to the DataFrame.')

        # Write the data to the OLAP database
        df.to_sql('ProductInStock_Fact', conn, schema=LND_SCHEMA, if_exists='fail', index=False)
        logger.info(f'Written the data to the ProductInStock_Fact OLAP database.')

        conn.commit()
        logger.info(f'Committed the changes to the ProductInStock_Fact OLAP database.')
    except Exception as e:
        logger.error(f'Error in mapping ProductInStock_Fact: {e}')