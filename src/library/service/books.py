from datetime import datetime, date, timedelta
import logging
from typing import List, Union
import pandas as pd
from sqlalchemy import text
import library.service.validations as validations
from library.persistence.database_connection import db

resource_name = "books"


def _handle_year(df: pd.DataFrame):
    current_year = datetime.now().year
    df = df[df['publication_year'] <= current_year]
    return df


def _handle_copies(df: pd.DataFrame):
    df = df[df['copies_available'] <= df['total_copies']]
    df = df[df['copies_available'] >= 0]
    return df


def _handle_isbn_duplicates(df: pd.DataFrame):
    return df.drop_duplicates(subset='isbn')


def _validate_author(df: pd.DataFrame):
    # query authors of books
    # if missing, again - log somewhere
    return df


def _validate_books(df: pd.DataFrame):
    required_cols = [
        'title',
        'author_id',
        'isbn',
        'publication_year',
        'genre',
        'copies_available',
        'total_copies',
    ]
    df = validations.validate_columns(df, required_cols, resource_name)
    df = validations.handle_duplicates(df, resource_name)
    df = validations.handle_missing_values(df, resource_name)
    df = _handle_year(df)
    df = _handle_copies(df)
    df = _handle_isbn_duplicates(df)
    df = _validate_author(df)
    return df


def _add_books(books_df: pd.DataFrame):
    df = _validate_books(books_df)
    if len(df) > 0:
        logging.info(f"saving {len(df)} {resource_name}")
        db.df_to_sql(resource_name, df)
    else:
        logging.info(f"no valid {resource_name} to save")


def add_book(books: Union[dict, List[dict]]):
    if isinstance(books, dict):
        df = pd.DataFrame([books])
    if isinstance(books, list):
        df = pd.DataFrame(books)
    _add_books(df)


def delete_book(id: int):
    with db.sqla_connection() as connection:
        result = connection.execute(
            text("SELECT id FROM books WHERE id = :id and deleted_at is null"),
            {"id": id}
        )
        if result.fetchone() is None:
            logging.warning(f"Book {id} not found")
            return False
        connection.execute(
            text("""
                UPDATE books
                SET deleted_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """),
            {"id": id}
        )
    logging.info(f"Deleted book: {id}")
    return True


def get_all_books_with_authors(from_id: int = 0, limit: int = 1000) -> pd.DataFrame:
    """
    Get all gook with information about authors
    Paginated
    """
    return db.sql_to_df(f"""
select * from books b 
join authors a on b.author_id = a.id
where b.id > {from_id}
limit {limit};
""")


# 4. CTE и рекурсивные запросы:
#    – Создание иерархического запроса для получения всех книг и их авторов
