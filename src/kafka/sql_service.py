import pandas as pd
from importlib.resources import files, as_file
from database.database_connection import db
import database.database_setup as ds

def init_sql():
    resource = files("kafka").joinpath("sql/init-tables.sql")
    with as_file(resource) as path:
        ds.execute_scripts(path)
