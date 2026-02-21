import os
from pathlib import Path

import psycopg2
import logging

def init_tables():
    logging.info('start init tables')
    init_tables_script_path = Path(__file__).parent / "sql" / "init-tables.sql"
    query = init_tables_script_path.read_text()
    with psycopg2.connect(
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            logging.info('tables initialized')