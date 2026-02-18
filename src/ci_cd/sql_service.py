import logging
from pathlib import Path
from typing import Any, Optional, Tuple
from database import database_setup
from database import database_connection

def init_tables():
    logging.info('initializing sql tables')
    init_script_path = Path(__file__).parent / "sql" / "init-tables.sql"
    database_setup.execute_scripts(str(init_script_path))
    logging.info('sql tables initialized')

def _data_exist(table_name):
    with database_connection.db.cursor() as cursor:
        cursor.execute(f"SELECT count(*) FROM {table_name}")
        row: Optional[Tuple[Any, ...]] = cursor.fetchone()
        if row:
            return int(row[0]) > 0
        else:
            return False

def init_data():
    logging.info('initializing data')
    if not _data_exist('customers'):
        logging.info('data does not exist. inserting.')
        init_script_path = Path(__file__).parent / "sql" / "init-data.sql"
        database_setup.execute_scripts(str(init_script_path))
    logging.info('data initialized')
