import logging
from pathlib import Path
from typing import Any, Optional, Tuple
from database import database_setup
from database import database_connection


def init_tables():
    logging.info('initializing sql tables')
    init_script_path = Path(__file__).parent / "sql" / "init-tables.sql"
    database_setup.execute_scripts(str(init_script_path))
    logging.info('sql tables initialized')


def _data_exist(table_name):
    with database_connection.db.cursor() as cursor:
        cursor.execute(f"SELECT count(*) FROM {table_name}")
        row: Optional[Tuple[Any, ...]] = cursor.fetchone()
        if row:
            return int(row[0]) > 0
        else:
            return False
        

def init_data():
    logging.info('initializing data')
    if not _data_exist('customers'):
        logging.info('data does not exist. inserting.')
        init_script_path = Path(__file__).parent / "sql" / "init-data.sql"
        database_setup.execute_scripts(str(init_script_path))
    logging.info('data initialized')


class MigrateMapping:
    def __init__(self, 
                 source_table='default_source_not_existing_table',
                 target_table='default_target_not_existing_table',
                 source_columns=[],
                 target_columns=[]):
        self.source_table = source_table
        self.target_table = target_table
        self.source_columns = source_columns
        self.target_columns = target_columns


def _construct_sql(mapping: MigrateMapping):
    return f"""
    INSERT INTO {mapping.target_table} ({", ".join(mapping.target_columns)}, loaded_date, record_source)
    SELECT {", ".join(mapping.source_columns)},
      current_timestamp as loaded_date,
      'raw-tables' as record_source
    FROM {mapping.source_table}
    """


def _move_from_table_to_table(mapping: MigrateMapping):
    with database_connection.db.cursor() as cursor:
        try:
            cursor.execute(_construct_sql(mapping))
        except Exception as e:
            logging.error(f"failed to migrate data from {mapping.source_table} to {mapping.target_table} {e}")


def _get_mappings():
    return [
        MigrateMapping(
            source_table="customers_raw",
            target_table="customers",
            source_columns=['customer_id'],
            target_columns=['customer_id'],
        ),
        MigrateMapping(
            source_table="products_raw",
            target_table="products",
            source_columns=['product_id', 'category_id'],
            target_columns=['product_id', 'category_id'],
        ),
        MigrateMapping(
            source_table="orders_raw",
            target_table="orders",
            source_columns=['order_id', 'customer_id', 'product_id', 'store_id'],
            target_columns=['order_id', 'customer_id', 'product_id', 'store_id'],
        ),
        MigrateMapping(
            source_table="stores_raw",
            target_table="stores",
            source_columns=['store_id'],
            target_columns=['store_id'],
        ),
        MigrateMapping(
            source_table="categories_raw",
            target_table="categories",
            source_columns=['category_id'],
            target_columns=['category_id'],
        ),
    ]

def migrate_data():
    for mapping in _get_mappings():
        logging.info(f"moving data from {mapping.source_table} to {mapping.target_table}")
        _move_from_table_to_table(mapping)
        logging.info(f"finished moving data from {mapping.source_table} to {mapping.target_table}")
