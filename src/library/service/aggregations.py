import logging
import pandas as pd
from sqlalchemy import text
from library.persistence.database_connection import db



def reader_author_union(limit_authors: int = 5, limit_readers: int = 5):
    if not isinstance(limit_authors, int) or not isinstance(limit_readers, int):
        raise ValueError("Limits must be integers")
    logging.info(f"saerching authors/readers")
    with db.sqla_connection() as connection:
        query = """
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
        logging.info(f"authors/readers found: {len(df)}")
        return df


def get_count_loan_over_book():
    logging.info(f"searching loans")
    with db.sqla_connection() as connection:
        query = f"""
                select *, 
                row_number() over(partition by book_id)
                from loans l 
                where l.deleted_at is null
                """
        result = connection.execute(text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        logging.info(f"loans found: {len(df)}")
        return df


def get_top_loaned_book_per_year_with_authors():
    logging.info(f"searching top loaned books per year")
    with db.sqla_connection() as connection:
        query = "drop MATERIALIZED VIEW IF EXISTS  book_loan_counts"
        result = connection.execute(text(query))
        query = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS book_loan_counts
AS (
    SELECT 
        b.id as book_id,
        b.publication_year,
        COUNT(l.id) as loan_count
    FROM books b
    JOIN loans l ON l.book_id = b.id
    where b.deleted_at is null
    and l.deleted_at is null
    GROUP BY b.id, b.publication_year
)
"""
        result = connection.execute(text(query))
        query = f"""
DROP INDEX if exists idx_book_loan_counts_year_loan
"""
        result = connection.execute(text(query))
        query = f"""
CREATE UNIQUE INDEX if not exists idx_book_loan_counts_year_loan 
ON book_loan_counts(publication_year, loan_count DESC, book_id);
"""
        result = connection.execute(text(query))
        query = f"""
with recursive years as (
	select min(b.publication_year) as publication_year
	from books b
	union all
	select publication_year + 1
	from years
	where publication_year < 2025
),
books_per_year as (
	select b.book_id
	from years y
    CROSS JOIN LATERAL (
	    SELECT blc.book_id book_id
	    FROM book_loan_counts blc
		where blc.publication_year = y.publication_year
		order by loan_count desc 
		limit 3
    ) b
)
select b.id, b.publication_year, a.id, b.title, a.id, (a.first_name || ' ' || a.last_name)
from books_per_year bpy
join books b on bpy.book_id = b.id
join authors a on b.author_id  = a.id
where b.deleted_at is null
and a.deleted_at is null
"""
        result = connection.execute(text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        logging.info(f"found top loaned books per year: {len(df)}")
        return df
