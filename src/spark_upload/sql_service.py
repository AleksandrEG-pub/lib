import database.database_setup as ds
from importlib.resources import files, as_file

def init_database():
    resource = files("spark_upload").joinpath("sql/init-tables.sql")
    with as_file(resource) as path:
        ds.execute_scripts(path)