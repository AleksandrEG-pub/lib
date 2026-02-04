import logging
from importlib.resources import files, as_file
from database.database_connection import db
import database.database_setup as ds

def init_tables():
    logging.info('initializing sql tables')
    resource = files("dbt_star").joinpath("sql/init-tables.sql")
    with as_file(resource) as path:
        ds.execute_scripts(path)
    logging.info('sql tables initialized')

def init_data():
    logging.info('initializing data')
    resource = files("dbt_star").joinpath("sql/init-data.sql")
    with as_file(resource) as path:
        ds.execute_scripts(path)
    logging.info('sql data initialized')
    
def update_data():
    logging.info('updating data')
    resource = files("dbt_star").joinpath("sql/update-data.sql")
    with as_file(resource) as path:
        ds.execute_scripts(path)
    logging.info('sql data updated')
    
def tables_empty():
    with db.cursor() as cursor:
        cursor.execute("""
            SELECT
                NOT (
                    EXISTS (SELECT 1 FROM customers) AND
                    EXISTS (SELECT 1 FROM products) AND
                    EXISTS (SELECT 1 FROM sales)
                );
        """)
        return cursor.fetchone()[0]

def populate_tables(file_name: str) -> int:
    pass