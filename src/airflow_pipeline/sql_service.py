import logging
from importlib.resources import files, as_file
from database.database_connection import db
import database.database_setup as ds

def init_sql():
    logging.info('initializing sql tables')
    resource = files("airflow_pipeline").joinpath("sql/init-tables.sql")
    with as_file(resource) as path:
        ds.execute_scripts(path)
    logging.info('sql tables initialized')

def count_lines_per_uploaded_file(file_name: str) -> int:
    with db.cursor() as cursor:
        cursor.execute("select count (bd.source_file) from bakery_deliveries bd  where bd.source_file = %s", (file_name,))
        row = cursor.fetchone()
        if row:
            return int(row[0])
