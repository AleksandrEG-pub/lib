import database.database_setup as ds
from database.database_connection import db
from importlib.resources import files, as_file

def init_database():
    resource = files("s3_upload").joinpath("sql/init-tables.sql")
    with as_file(resource) as path:
        ds.execute_scripts(path)

def get_database_url():
    config = db.config
    connection_string = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
    return connection_string