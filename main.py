# Tables and Layers creation
# loading data from OLTP to Landing
# calling the merge stored procedure from the Landing to Staging
# calling the merge from the Staging to the Data Warehouse (SCD 2)
# Truncate the necessary tables

from sqlalchemy import create_engine

from OLTPtoOLAP import load_dimension_tables, load_fact_tables
from StoredProcedure import execute_merge_proc
from SQLRunner import run_sql_file
from Config import logger, CONNECTION_URL, OLTP_SCHEMA, LND_SCHEMA, STG_SCHEMA, DW_SCHEMA, DIM_TABLES, FACT_TABLES, MATCHING_CONDITIONS, REFRESH_TABLES, REFRESH_PROCEDURES

def main():
    # Create a connection to the OLTP database
    engine = create_engine(CONNECTION_URL)
    logger.info('Created engine to connect to the OLTP database.')

    try:
        # Read the data from the OLTP database by obtaining the connection
        with engine.connect() as conn:
            logger.info('Connected to the OLTP database.')

            #Create the required schemas and its tables
            if REFRESH_TABLES:
                logger.info('Creating the required schemas and its tables.')
                run_sql_file(conn, 'Landing_TableCreation.sql')
                run_sql_file(conn, 'Staging_TableCreation.sql')
                run_sql_file(conn, 'DW_TableCreation.sql')
                logger.info('Finished creating the required schemas and its tables.')

            # Create the required stored procedures
            if REFRESH_PROCEDURES:
                logger.info('Creating the required stored procedures.')
                run_sql_file(conn, 'LandingToStaging_MergeProc.sql')
                run_sql_file(conn, 'StagingToDW_MergeProc.sql')
                logger.info('Finished creating the required stored procedures.')

            # Mapping the OLTP tables to OLAP tables
            logger.info('Starting mapping of OLTP tables to OLAP Landing tables.')

            # Load the dimension tables
            load_dimension_tables(conn)

            # TODO: Load the bridge tables instead of fact tables
            # Load the fact tables
            # load_fact_tables(conn)

            logger.info('Finished mapping of OLTP tables to OLAP Landing tables.')
            logger.info('-' * 100)

            # Execute the AutoSCD1 stored procedure to merge the data from Landing to Staging for each dimension table
            logger.info('Executing the AutoSCD1 stored procedure to merge the data from Landing to Staging for each dimension table.')
            for dt in DIM_TABLES:
                execute_merge_proc(
                    conn,
                    f'{LND_SCHEMA}.AutoSCD1',
                    f'{LND_SCHEMA}.{dt}',
                    f'{STG_SCHEMA}.{dt}',
                    MATCHING_CONDITIONS[dt]
                )

            # Execute the AutoSCD2 stored procedure to merge the data from Staging to Data Warehouse for each dimension table
            logger.info('Executing the AutoSCD2 stored procedure to merge the data from Staging to Data Warehouse for each dimension table.')
            for dt in DIM_TABLES:
                execute_merge_proc(
                    conn,
                    f'{STG_SCHEMA}.AutoSCD2',
                    f'{STG_SCHEMA}.{dt}',
                    f'{DW_SCHEMA}.{dt}',
                    MATCHING_CONDITIONS[dt]
                )

    except Exception as e:
        logger.error(f'Error in the main function: {e.__class__.__name__}: {e}')


if __name__ == '__main__':
    main()