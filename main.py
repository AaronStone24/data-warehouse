# Tables and Layers creation
# loading data from OLTP to Landing
# calling the merge stored procedure from the Landing to Staging
# calling the merge from the Staging to the Data Warehouse (SCD 2)
# Truncate the necessary tables

from sqlalchemy import create_engine

from OLTPtoOLAP import load_dimension_tables, load_fact_tables
from StoredProcedure import execute_merge_proc
from Constants import logger, CONNECTION_URL, OLTP_SCHEMA, LND_SCHEMA, STG_SCHEMA, DW_SCHEMA, DIM_TABLES, FACT_TABLES

def main():
    # Create a connection to the OLTP database
    engine = create_engine(CONNECTION_URL)
    logger.info('Created engine to connect to the OLTP database.')

    try:
        # Read the data from the OLTP database by obtaining the connection
        with engine.connect() as conn:
            logger.info('Connected to the OLTP database.')

            # Mapping the OLTP tables to OLAP tables
            logger.info('Starting mapping of OLTP tables to OLAP tables.')

            # Load the dimension tables
            load_dimension_tables(conn)

            # TODO: Load the bridge tables instead of fact tables
            # Load the fact tables
            load_fact_tables(conn)

            logger.info('Finished mapping of OLTP tables to OLAP tables.')
            logger.info('-' * 100)

            # Execute the stored procedure to merge the data from Landing to Staging for each dimension table
            logger.info('Executing the stored procedure to merge the data from Landing to Staging for each dimension table.')
            for dt in DIM_TABLES:
                t = dt.split('_')[0]
                execute_merge_proc(
                    conn,
                    'AutoSCD1',
                    f'{LND_SCHEMA}.{dt}',
                    f'{STG_SCHEMA}.{dt}',
                    f'TGT.{t}Key=SRC.{t}Key'
                )

    except Exception as e:
        logger.error(f'Error in the main function: {e.__class__.__name__}: {e}')


if __name__ == '__main__':
    main()