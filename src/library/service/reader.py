import logging
from typing import List, Union
import pandas as pd
from sqlalchemy import text
import library.service.validations as validations
from library.persistence.database_connection import db


resource_name = "readers"


def _validate_reader(df: pd.DataFrame):
    required_cols = ['first_name', 'last_name', 'email']
    df = validations.validate_columns(df, required_cols, resource_name)
    df = validations.handle_duplicates(df, resource_name)
    # validate email
    df = validations.handle_missing_values(df, resource_name)
    return df


def _add_reader(readers_df: pd.DataFrame):
    df = _validate_reader(readers_df)
    if not df.empty:
        logging.info(f"saving {len(df)} {resource_name}")
        db.df_to_sql(resource_name, df)
    else:
        logging.info("no valid {resource_type} to save")


def add_reader(readers: Union[dict, List[dict]]):
    if isinstance(readers, dict):
        author_df = pd.DataFrame([readers])
    if isinstance(readers, list):
        author_df = pd.DataFrame(readers)
    _add_reader(author_df)


def update_reader(id: int, reader: dict):
    reader_properties = {'first_name', 'last_name'}
    if set(reader.keys()) != reader_properties:
        logging.error(f"Can only update: {reader_properties}")
        return False
    
    with db.sqla_connection() as connection:
        result = connection.execute(
            text("SELECT id FROM readers WHERE id = :id and deleted_at is null"),
            {"id": id}
        )
        if result.fetchone() is None:
            logging.warning(f"Reader {id} not found")
            return False
        connection.execute(
            text("""
                UPDATE readers 
                SET first_name = :first_name, last_name = :last_name
                WHERE id = :id
            """),
            {
                "first_name": reader["first_name"],
                "last_name": reader["last_name"],
                "id": id
            }
        )
    logging.info(f"Updated reader: {id}")
    return True
