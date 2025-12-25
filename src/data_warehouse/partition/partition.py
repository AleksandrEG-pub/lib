import logging
import database.database_setup as db_setup
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
init_partitions_file = f"{script_dir}/sql/init_partitions.sql"
insert_data_file = f"{script_dir}/sql/insert_data.sql"
insert_data_procedure_file = f"{script_dir}/sql/insert_data_procedure.sql"
query_data_file = f"{script_dir}/sql/query_data.sql"


def setup_database() -> bool:
    logging.info('setting up database')
    result = db_setup.execute_scripts(init_partitions_file)
    logging.info("tables set up")
    return result

def insert_data() -> bool:
    logging.info('inserting data to partitions')
    result = db_setup.execute_script_all(insert_data_procedure_file)
    result = db_setup.execute_scripts(insert_data_file)
    logging.info('data insertion to partitions done')
    return result

def query_data() -> bool:
    logging.info('quering data from partitions')
    db_setup.execute_scripts(query_data_file)
    logging.info('done')

