import sqlalchemy

from Constants import logger
from Exceptions import MergeError, TableDoesNotExistError

def execute_merge_proc(conn: sqlalchemy.Connection, merge_proc_name, source_table, target_table, matching_condition):
    logger.info(f'Executing the stored procedure to merge the data from {source_table} to {target_table}.')
    try:
        cursor = conn.connection.cursor()
        sql = f"EXEC {merge_proc_name} @SourceTable=?, @TargetTable=?, @matching_condition=?"
        params = (source_table, target_table, matching_condition)
        result = cursor.execute(sql, params).fetchone()[0]
        return_code = int(result.split(',')[0].strip())
        if return_code == 208:
            raise TableDoesNotExistError(result.split(',')[1].strip())
        elif return_code == 0:
            pass
        else:
            raise MergeError(merge_proc_name, f'Error in executing the merge stored procedure {merge_proc_name}: {result}')
        conn.commit()
        logger.info(f'Successfully executed the stored procedure to merge the data from {source_table} to {target_table}.')

    except TableDoesNotExistError as e:
        logger.error(f'TableDoesNotExistError: {e}')
    except Exception as e:
        logger.error(f'Error in executing the merge stored procedure {merge_proc_name} for {source_table} to {target_table}: {e}')
        raise MergeError(merge_proc_name, f'Error in executing the merge stored procedure {merge_proc_name}: {e}')
    finally:
        cursor.close()