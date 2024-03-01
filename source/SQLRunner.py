import os
import sqlalchemy

from .Config import logger
from .utils.Exceptions import SqlError

def run_sql_file(conn: sqlalchemy.Connection, file_name: str):
    cursor = None
    try:
        file_path = os.path.join(os.getcwd(), 'sql', file_name)

        # check if the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f'File {file_path} does not exist.')
        
        logger.info(f'Executing the SQL file: {file_name}')
        with open(file_path, 'r') as file:
            sql_code = file.read()
            if 'GO\n' in sql_code:
                sql_commands = sql_code.split('GO\n')
                for i, cmd in enumerate(sql_commands):
                    params = ()
                    if i == len(sql_commands) - 1 or i == 0:
                        run_sql_code(conn, cmd, params)
                    else:
                        run_sql_code(conn, cmd, params, no_result=True)
            else:
                params = ()
                run_sql_code(conn, sql_code, params)
            logger.info(f'Executed the SQL file: {file_name}')

    except FileNotFoundError as e:
        logger.error(e)
        raise e
    except Exception as e:
        logger.error(f'Error in executing the SQL file {file_name}: {e}')
        raise e
    
    finally:
        if cursor is not None:
            cursor.close()

def run_sql_code(conn: sqlalchemy.Connection, sql_code: str, params: tuple, no_result=False):
    try:
        cursor = conn.connection.cursor()
        if no_result:
            cursor.execute(sql_code, params)
        else:
            result = cursor.execute(sql_code, params).fetchone()[0]
            return_code = int(result.split(',')[0].strip())
            return_msg = result.split(',')[1].strip()
            if return_code != 0:
                raise SqlError(return_code, return_msg)
            else:
                logger.info(return_msg)
        conn.commit()
    except SqlError as e:
        raise e
    except Exception as e:
        raise e
    finally:
        cursor.close()

'''
from Config import CONNECTION_URL
from sqlalchemy import create_engine
engine = create_engine(CONNECTION_URL)
with engine.connect() as conn:
    run_sql_file(conn, 'Staging_TableCreation.sql')
'''
