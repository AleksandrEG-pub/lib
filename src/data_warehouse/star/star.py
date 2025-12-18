import logging
import database.database_setup as db_setup
import os


script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
tables_sql_file = f"{script_dir}/sql/init_star.sql"
data_sql_file = f"{script_dir}/sql/insert_data.sql"
update_sql_file = f"{script_dir}/sql/update_data.sql"


def setup_database() -> bool:
    logging.info('setting up database')
    result = db_setup.execute_scripts(tables_sql_file)
    logging.info("tables set up")
    return result

def insert_data():
    logging.info("insert new data")
    result = db_setup.execute_scripts(data_sql_file)
    logging.info("data inserted")
    return result

def updates():
    logging.info("update data")
    result = db_setup.execute_scripts(update_sql_file)
    logging.info("data updated")
    return result
