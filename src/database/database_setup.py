import os
import psycopg2
import logging
from database.database_connection import db

def setup_database(sql_scripts_file) -> bool:
    logging.info('setting up database')
    with open(sql_scripts_file, 'r') as file:
        sql_commands = ''
        try:
            sql_commands = file.read()
        except FileNotFoundError:
            logging.error(f"failed to read init scripts from file {sql_scripts_file}")
            return False
        with db.cursor() as cursor:
            cursor.execute(sql_commands)
            logging.info("tables set up")
            if cursor.rowcount > 0:
                logging.info(f"updated {cursor.rowcount}")
    return True
