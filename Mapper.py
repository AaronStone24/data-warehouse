import pandas as pd
import numpy as np

from Constants import SRC_SCHEMA, TGT_SCHEMA

# TODO: Error Handling
# TODO: Check if the OLAP table is already filled or not

def oltp_to_olap_mapping(oltp_table_name, olap_table_name, mapping: dict, conn):
    '''
    Function to map the OLTP tables to OLAP tables
    :param oltp_table_name: Name of the OLTP table
    :param olap_table_name: Name of the OLAP table
    :param mapping: Dictionary with the mapping of columns from OLTP to OLAP
    :param conn: Connection to the OLTP database
    '''
    oltp_columns, olap_columns = list(mapping.keys()), list(mapping.values())

    sql_query = f'SELECT {','.join(oltp_columns)} FROM {SRC_SCHEMA}.{oltp_table_name}'
    if olap_table_name == 'Calendar_Dim':
        sql_query = f'SELECT DISTINCT {oltp_columns[0]} FROM {SRC_SCHEMA}.{oltp_table_name}'
    oltp_df = pd.read_sql_query(sql_query, conn)
    print(oltp_df.head())

    # Replace the missing values with NaN
    oltp_df.replace('', np.NaN, inplace=True)

    # Rename the columns
    oltp_df = oltp_df.rename(columns=mapping)

    # Special case for Calendar_Dim
    if olap_table_name == 'Calendar_Dim':
        oltp_df['DayofTheWeek'] = oltp_df['FullDate'].dt.day_name()
        oltp_df['DayType'] = np.where(oltp_df['DayofTheWeek'].isin(['Saturday', 'Sunday']), 'Weekend', 'Weekday')
        oltp_df['DayofTheMonth'] = oltp_df['FullDate'].dt.day
        oltp_df['Month'] = oltp_df['FullDate'].dt.month
        oltp_df['Quarter'] = oltp_df['FullDate'].dt.quarter
        oltp_df['Year'] = oltp_df['FullDate'].dt.year

    # Add the SourceTable column to the DataFrame
    oltp_df['SourceTable'] = f'{SRC_SCHEMA}.{oltp_table_name}'
    # Add the loadTimeDate column to the DataFrame
    oltp_df['loadTimeDate'] = pd.to_datetime('now')

    # Write the data to the OLAP database
    oltp_df.to_sql(olap_table_name, conn, schema=TGT_SCHEMA, if_exists='append', index=False)
    conn.commit()

def customerEmployee_Fact_mapper(conn):
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
        GROUP BY CustomerKey, EmployeeKey, CalendarKey, OD.OrderID'''.format(SRC_SCHEMA, TGT_SCHEMA)
    
    df = pd.read_sql_query(sql_query, conn)
    print(df.head())

    # Replace the missing values with NaN
    df.replace('', np.NaN, inplace=True)

    # Add the SourceTable column to the DataFrame
    df['SourceTable'] = f'{SRC_SCHEMA}.Orders, {SRC_SCHEMA}.OrderDetails'
    # Add the loadTimeDate column to the DataFrame
    df['loadTimeDate'] = pd.to_datetime('now')

    # Write the data to the OLAP database
    df.to_sql('CustomerEmployee_Fact', conn, schema=TGT_SCHEMA, if_exists='append', index=False)
    conn.commit()

def ProductInStock_Fact_mapper(conn):
    sql_query = '''SELECT CalendarKey, ProductKey, CategoriesKey, SupplierKey, UnitsInStock, UnitsOnOrder, ReorderLevel,
    SUM(Quantity) OVER (PARTITION BY ODT.ProductID) AS TotalQuantity,
    OD.OrderID
    FROM {0}.Orders OD
    JOIN {0}.[Order Details] ODT ON OD.OrderID = ODT.OrderID
    JOIN {0}.Products PDT ON ODT.ProductID = PDT.ProductID
    JOIN {1}.Calendar_Dim CLD ON OD.OrderDate = CLD.FullDate
    JOIN {1}.Product_Dim PD ON ODT.ProductID = PD.ProductID
    JOIN {1}.Categories_Dim CD ON PDT.CategoryID = CD.CategoryID
    JOIN {1}.Supplier_Dim SD ON PDT.SupplierID = SD.SupplierID'''.format(SRC_SCHEMA, TGT_SCHEMA)

    df = pd.read_sql_query(sql_query, conn)
    print(df.head())

    # Replace the missing values with NaN
    df.replace('', np.NaN, inplace=True)

    # Add the SourceTable column to the DataFrame
    df['SourceTable'] = f'{SRC_SCHEMA}.Orders, {SRC_SCHEMA}.OrderDetails, {SRC_SCHEMA}.Products'
    # Add the loadTimeDate column to the DataFrame
    df['loadTimeDate'] = pd.to_datetime('now')

    # Write the data to the OLAP database
    df.to_sql('ProductInStock_Fact', conn, schema=TGT_SCHEMA, if_exists='append', index=False)
    conn.commit()