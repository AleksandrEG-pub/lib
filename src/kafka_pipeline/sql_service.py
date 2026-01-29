import logging
from importlib.resources import files, as_file
from database.database_connection import db
import database.database_setup as ds

def init_sql():
    logging.info('initializing sql tables')
    resource = files("kafka_pipeline").joinpath("sql/init-tables.sql")
    with as_file(resource) as path:
        ds.execute_scripts(path)
    logging.info('sql tables initialized')
