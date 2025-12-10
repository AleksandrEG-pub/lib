import logging
import pandas as pd
from sqlalchemy import text
from library.persistence.database_connection import db

def get_loans_by_return_date(limit_authors: int = 100, limit_readers: int = 100):
    """Выбор всех заказов с указанием даты возврата или значения по умолчанию"""
    with db.sqla_connection() as connection:
        query = f"""
            (select a.first_name, a.last_name, 'author' as role
            from authors a 
            limit :limit_authors)
            union 
            (select r.first_name, r.last_name, 'reader' as role
            from readers r 
            limit :limit_readers);
            """
        result = connection.execute(
            text(query),
            {"limit_authors": limit_authors, "limit_readers": limit_readers}
        )
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df