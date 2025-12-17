from typing import List, Union
import pandas as pd
import library.service.validations as validations
from library.persistence.database_connection import db
import logging


def _handle_dates(df: pd.DataFrame):
    original_size = len(df)
    df['birth_date'] = pd.to_datetime(
        df['birth_date'], format='%Y-%m-%d', errors='coerce')
    removed_rows = original_size - len(df) > 0
    if(removed_rows > 0):
        logging.info(f'filtered {removed_rows} by wrong birth_date')
    return df


def _validate_author(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ['first_name', 'last_name', 'birth_date']
    df = validations.validate_columns(df, required_cols, 'author')
    df = validations.handle_duplicates(df, 'author')
    df = _handle_dates(df)
    df = validations.handle_missing_values(df, 'author')
    return df


def _add_authors(authors_df: pd.DataFrame):
    df = _validate_author(authors_df)
    if not df.empty:
        logging.info(f"saving {len(df)} authors")
        db.df_to_sql('authors', df)
    else:
        logging.info("no valid authors to save")


def add_author(author: Union[dict, List[dict]]):
    if isinstance(author, dict):
        author_df = pd.DataFrame([author])
    if isinstance(author, list):
        author_df = pd.DataFrame(author)
    _add_authors(author_df)
