import pandas as pd

from Constants import SRC_SCHEMA, TGT_SCHEMA

def oltp_to_olap_mapping(oltp_table, olap_table, mapping: dict, conn):
    '''
    Function to map the OLTP tables to OLAP tables
    '''
    oltp_columns, olap_columns = mapping.keys(), mapping.values()

    sql_query = f'SELECT {oltp_columns} FROM {SRC_SCHEMA}.{oltp_table}'
    oltp_data = pd.read_sql_query(sql_query, conn)
    print(oltp_data.head())