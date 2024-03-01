import sqlalchemy

from Config import logger
from utils.Exceptions import MergeError, TableDoesNotExistError, SqlError
from SQLRunner import run_sql_code

def execute_merge_proc(conn: sqlalchemy.Connection, merge_proc_name, source_table, target_table, matching_condition):
    logger.info(f'Executing the stored procedure to merge the data from {source_table} to {target_table}.')
    try:
        cursor = conn.connection.cursor()
        sql = f"EXEC {merge_proc_name} @SourceTable=?, @TargetTable=?, @matching_condition=?"
        params = (source_table, target_table, matching_condition)
        run_sql_code(conn, sql, params)
        logger.info(f'Successfully executed the stored procedure to merge the data from {source_table} to {target_table}.')
        # conn.commit()
    
    except SqlError as e:
        return_code = e.return_code
        return_msg = e.message
        if return_code == 208:
            logger.error(f'TableDoesNotExistError: {e}')
        else:
            raise MergeError(merge_proc_name, f'Error in executing the merge stored procedure {merge_proc_name}: {return_msg}')        
    except Exception as e:
        logger.error(f'Error in executing the merge stored procedure {merge_proc_name} for {source_table} to {target_table}: {e}')
        raise MergeError(merge_proc_name, f'Error in executing the merge stored procedure {merge_proc_name}: {e}')
    finally:
        cursor.close()