import logging
import database.database_setup as db_setup
import os


script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
tables_sql_file = f"{script_dir}/sql/vault_init.sql"


def setup_database() -> bool:
    logging.info('setting up data vault')
    result = db_setup.execute_scripts(tables_sql_file)
    logging.info("data vault set up")
    return result
