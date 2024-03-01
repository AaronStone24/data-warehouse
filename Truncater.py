from SQLRunner import run_sql_code
from Config import logger

def truncate_table(conn, tableName):
    try:
        logger.info(f'Truncating the table {tableName}.')
        sql_code = f'TRUNCATE TABLE {tableName}'
        params = ()
        run_sql_code(conn, sql_code, params, no_result=True)
        logger.info(f'Truncated the table {tableName}.')
    except Exception as e:
        logger.error(f'Error while truncating {tableName}: {e}')
        raise e