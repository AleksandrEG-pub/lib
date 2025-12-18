import random
from typing import Callable, Dict, Union
from datetime import datetime as dt_class, timedelta
from library.persistence.database_connection import db

from faker import Faker
fake = Faker()

LOAN_START_RANGE = '-25y'

def _generate_loan_dates() -> Dict[str, Union[str, None]]:
    """Generate loan dates with return probability"""
    loan_date = fake.date_between(
        start_date=LOAN_START_RANGE,
        end_date='today'
    )
    due_date = loan_date + timedelta(days=random.randint(14, 60))
    # 95% of loans are returned
    if random.random() < 0.95:
        return_date = loan_date + timedelta(days=random.randint(1, 90))
        return_date_str = return_date.isoformat()
    else:
        return_date_str = None
    return {
        'loan_date': loan_date.isoformat(),
        'due_date': due_date.isoformat(),
        'return_date': return_date_str
    }

def _get_rnd_book_id_handler(book_limit: int = 1000):
    query = f"SELECT id FROM books LIMIT {book_limit}"
    book_ids = db.sql_to_df(query)['id'].tolist()
    def get_next():
        return random.choice(book_ids)
    return get_next


def _get_rnd_reader_id_handler(reader_limit: int = 1000):
    query = f"SELECT id FROM readers LIMIT {reader_limit}"
    book_ids = db.sql_to_df(query)['id'].tolist()
    def get_next():
        return random.choice(book_ids)
    return get_next


def make_loan_factory() -> Callable[[None], Dict]:
    book_gen = _get_rnd_book_id_handler()
    reader_gen = _get_rnd_reader_id_handler()
    def make_loan() -> Dict:
        dates = _generate_loan_dates()
        return {
            'book_id': book_gen(),
            'reader_id': reader_gen(),
            **dates
        }
    return make_loan