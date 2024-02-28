from Constants import logger

def execute_merge_proc(conn, merge_proc_name, source_table, target_table, matching_condition):
    try:
        cursor = conn.connection.cursor()
        sql = f"EXEC {merge_proc_name} @SourceTable=?, @TargetTable=?, @matching_condition=?"
        params = (source_table, target_table, matching_condition)
        cursor.execute(sql, params)
        if cursor.rowcount > 0:
            print(f'{cursor.rowcount} rows affected.')
        conn.commit()
    except Exception as e:
        logger.error(f'Error in executing the merge stored procedure {merge_proc_name}: {e}')
    finally:
        cursor.close()
        logger.info(f'Executed the stored procedure to merge the data from {source_table} to {target_table}.')