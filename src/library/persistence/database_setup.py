import os
import psycopg2
import logging
from library.persistence.database_connection import db

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
tables_sql_file = f"{script_dir}/sql/tables.sql"


def setup_database():
    logging.info('setting up database')
    with open(tables_sql_file, 'r') as file:
        sql_commands = ''
        try:
            sql_commands = file.read()
        except FileNotFoundError:
            logging.error(f"failed to read init scripts from file {tables_sql_file}")
        with db.cursor() as cursor:
            cursor.execute(sql_commands)
            logging.info("tables created")
            if cursor.rowcount > 0:
                logging.info(f"updated {cursor.rowcount}")

