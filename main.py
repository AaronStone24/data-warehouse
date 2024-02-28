# Tables and Layers creation
# loading data from OLTP to Landing
# calling the merge stored procedure from the Landing to Staging
# calling the merge from the Staging to the Data Warehouse (SCD 2)
# Truncate the Landing tables

import os
from sqlalchemy import create_engine

from OLTPtoOLAP import load_dimension_tables, load_fact_tables
from Constants import logger, CONNECTION_URL

#TODO: Error Handling

def main():
    # Create a connection to the OLTP database
    engine = create_engine(CONNECTION_URL)
    logger.info('Created engine to connect to the OLTP database.')

    # Read the data from the OLTP database by obtaining the connection
    with engine.connect() as conn:
        logger.info('Connected to the OLTP database.')

        # Mapping the OLTP tables to OLAP tables
        logger.info('Starting mapping of OLTP tables to OLAP tables.')

        # Load the dimension tables
        load_dimension_tables(conn)

        # Load the fact tables
        load_fact_tables(conn)

        logger.info('Finished mapping of OLTP tables to OLAP tables.')
        logger.info('-' * 50)

        # Obtain the merge stored procedure from the sql file LandingToStaging_MergeProc.sql
        # and execute it for each table
        # sql_file_path = os.path.join(os.getcwd(), 'sql', 'LandingToStaging_MergeProc.sql')
        # sql_code = sql_file.read()
        try:
            cursor = conn.connection.cursor()
            sql = "EXEC AutoSCD1 @SourceTable=?, @TargetTable=?, @matching_condition=?"
            params = ('DW_Landing.Categories_Dim', 'DW_Staging.Categories_Dim', 'TGT.CategoriesKey=SRC.CategoriesKey')
            cursor.execute(sql, params)
            if cursor.rowcount > 0:
                print(f'{cursor.rowcount} rows affected.')
        except Exception as e:
            logger.error(f'Error in executing the stored procedure: {e}')
        finally:
            cursor.close()


if __name__ == '__main__':
    main()