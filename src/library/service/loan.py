from datetime import datetime, date, timedelta
import pandas as pd
from typing import List, Union
import logging

from sqlalchemy import text
import library.service.validations as validations
from library.persistence.database_connection import db

resource_name = "loans"


def _validate_due_date(df: pd.DataFrame):
    original_size = len(df)
    df['due_date'] = pd.to_datetime(
        df['due_date'], format='%Y-%m-%d', errors='coerce')
    today = datetime.today()
    df = df[df['due_date'] < today]
    removed_rows = original_size - len(df) > 0
    if(removed_rows > 0):
        logging.info(f'filtered {removed_rows} by wrong due_date')
    return df


def _validate_book(df: pd.DataFrame):
    unique_book_ids = df['book_id'].dropna().unique().tolist()
    if not unique_book_ids:
        return df
    with db.sqla_connection() as connection:
        result = connection.execute(
            text("""
                SELECT id 
                FROM books 
                WHERE id IN :ids 
                AND deleted_at IS NULL
            """),
            {"ids": tuple(unique_book_ids)}
        )
        valid_book_ids = {row[0] for row in result.fetchall()}
        invalid_book_ids = set(unique_book_ids) - valid_book_ids
        for invalid_id in invalid_book_ids:
            logging.warning(f"Book ID {invalid_id} not found or is deleted")
        valid_df = df[df['book_id'].isin(valid_book_ids)]
        invalid_count = len(df) - len(valid_df)
        if invalid_count > 0:
            logging.info(f"Filtered out {invalid_count} rows with invalid book IDs")
        return valid_df


def _validate_reader(df: pd.DataFrame):
    unique_reader_ids = df['reader_id'].dropna().unique().tolist()
    if not unique_reader_ids:
        return df
    with db.sqla_connection() as connection:
        result = connection.execute(
            text("""
                SELECT id 
                FROM readers 
                WHERE id IN :ids 
                AND deleted_at IS NULL
            """),
            {"ids": tuple(unique_reader_ids)}
        )
        valid_reader_ids = {row[0] for row in result.fetchall()}
        invalid_reader_ids = set(unique_reader_ids) - valid_reader_ids
        for invalid_id in invalid_reader_ids:
            logging.warning(f"Reader ID {invalid_id} not found or is deleted")
        valid_df = df[df['reader_id'].isin(valid_reader_ids)]
        invalid_count = len(df) - len(valid_df)
        if invalid_count > 0:
            logging.info(f"Filtered out {invalid_count} rows with invalid reader IDs")
        return valid_df


def _validate_loans(df: pd.DataFrame):
    required_cols = [
        'book_id',
        'reader_id',
        'due_date',
    ]
    df = validations.validate_columns(df, required_cols, resource_name)
    df = validations.handle_duplicates(df, resource_name)
    df = validations.handle_missing_values(df, resource_name)
    df = _validate_due_date(df)
    df = _validate_book(df)
    df = _validate_reader(df)
    return df


def _add_loans(loans_df: pd.DataFrame):
    df = _validate_loans(loans_df)
    if not df.empty:
        logging.info(f"saving {len(df)} {resource_name}")
        db.df_to_sql(resource_name, df)
    else:
        logging.info("no valid {resource_type} to save")


def add_loan(loans: Union[dict, List[dict]]):
    if isinstance(loans, dict):
        df = pd.DataFrame([loans])
    if isinstance(loans, list):
        df = pd.DataFrame(loans)
    _add_loans(df)


def get_loans_by_return_date():
    logging.info(f"searching loans with return date")
    query = """
    select id, book_id, reader_id, loan_date, due_date,
        coalesce (return_date, due_date + INTERVAL '7 days') as return_date
    from loans
    where deleted_at is null
    """
    result = db.sql_to_df(query=query)
    logging.info(f"found loans with return date: [{len(result)}]")
    return result 
