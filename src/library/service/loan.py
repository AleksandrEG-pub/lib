from datetime import datetime, date, timedelta
import pandas as pd
from typing import List, Union
import logging
import library.service.validations as validations
from library.persistence.database_connection import db

resource_name = "loans"


def _validate_due_date(df: pd.DataFrame):
    df['due_date'] = pd.to_datetime(
        df['due_date'], format='%Y-%m-%d', errors='coerce')
    today = datetime.today()
    df = df[df['due_date'] < today]
    return df


def _validate_book(df: pd.DataFrame):
    # query if books exist
    return df


def _validate_reader(df: pd.DataFrame):
    # query if reader exist
    return df


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
    pass


def get_loans_dayly():
    pass
