import pandas as pd
import numpy as np

from Constants import SRC_SCHEMA, TGT_SCHEMA

# TODO: Error Handling

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
    oltp_df = pd.read_sql_query(sql_query, conn)
    print(oltp_df.head())

    # Replace the missing values with NaN
    oltp_df.replace('', np.NaN, inplace=True)

    # Rename the columns
    oltp_df = oltp_df.rename(columns=mapping)

    # Add the SourceTable column to the DataFrame
    oltp_df['SourceTable'] = f'{SRC_SCHEMA}.{oltp_table_name}'

    # Write the data to the OLAP database
    oltp_df.to_sql(olap_table_name, conn, schema=TGT_SCHEMA, if_exists='replace', index=False)
    conn.commit()
