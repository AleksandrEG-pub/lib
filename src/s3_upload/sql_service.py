import os
import database.database_setup as ds
from database.database_connection import db


script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
init_tables = f"{script_dir}/sql/init-tables.sql"
init_data = f"{script_dir}/sql/init-data.sql"


def init_database():
    ds.execute_scripts(init_tables)
    ds.execute_scripts(init_data)


def get_database_url():
    config = db.config
    connection_string = f'postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}'
    return connection_string