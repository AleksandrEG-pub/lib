import pandas as pd
from faker import Faker
from datetime import datetime as dt_class, timedelta
import datetime as dt_module
import random
from typing import List, Dict, Optional, Union, Iterator
import library.service.author as author
import library.service.reader as readers
import library.service.books as books
import library.service.loan as loan
from library.persistence.database_connection import db
import itertools

fake = Faker()


class DataGenerator:
    """Generic batch data generator with common utilities"""
    
    # Shared constants
    DOMAINS = ['gmail.com', 'yandex.com', 'mail.com', 'example.com']
    GENRES = ['Fiction', 'Non-Fiction', 'Science', 'Fantasy', 'Mystery',
              'Biography', 'History', 'Romance', 'Technology', 'Children']
    
    # Date ranges
    AUTHOR_BIRTH_START = dt_module.date(1678, 1, 1)
    AUTHOR_BIRTH_END = dt_module.date(2000, 12, 31)
    LOAN_START_RANGE = '-25y'
    
    @staticmethod
    def generate_in_batches(total: int, batch_size: int) -> Iterator[int]:
        """Generator for batch ranges"""
        for i in range(0, total, batch_size):
            yield min(batch_size, total - i)
    
    @staticmethod
    def chunk_list(lst: List, chunk_size: int) -> Iterator[List]:
        """Split list into chunks"""
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]


# ================ AUTHORS ====================
def _generate_authors_batch(batch_size: int) -> List[Dict]:
    """Generate a batch of author data"""
    authors = []
    for _ in range(batch_size):
        date_of_birth = fake.date_between(
            start_date=DataGenerator.AUTHOR_BIRTH_START,
            end_date=DataGenerator.AUTHOR_BIRTH_END
        )
        authors.append({
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'birth_date': date_of_birth.isoformat()
        })
    return authors


def generate_authors(total_authors: int = 10000, batch_size: int = 1000) -> None:
    """Generate authors in batches"""
    generated = 0
    for batch_num, current_batch_size in enumerate(DataGenerator.generate_in_batches(total_authors, batch_size), 1):
        batch = _generate_authors_batch(current_batch_size)
        author.add_author(batch)
        generated += len(batch)
        
        if batch_num % 10 == 0:
            print(f"Generated {generated}/{total_authors} authors...")
    
    print(f"✅ Generated {generated} authors")

ISBN_START = 178_000_000_000
# ================ BOOKS ====================
def _generate_isbn() -> str:
    """Generate a valid ISBN-13"""
    global ISBN_START
    ISBN_START += 1
    return str(ISBN_START)


def _generate_book_title() -> str:
    """Generate a book title"""
    return f"{fake.catch_phrase().title()} {fake.word().title()}"


def _generate_books_batch(batch_size: int, author_ids: List[int]) -> List[Dict]:
    """Generate a batch of book data"""
    return [{
        'title': _generate_book_title(),
        'author_id': random.choice(author_ids),
        'isbn': _generate_isbn(),
        'publication_year': random.randint(1950, 2023),
        'genre': random.choice(DataGenerator.GENRES),
        'copies_available': random.randint(1, 10),
        'total_copies': random.randint(1, 10)
    } for _ in range(batch_size)]


def generate_books(
    total_books: int = 50_000_000,
    batch_size: int = 50_000,
    author_chunk_size: int = 1000
) -> None:
    """Generate books in batches with optimized author ID handling"""
    
    # Get all author IDs once
    print("Fetching author IDs...")
    author_ids = db.sql_to_df("SELECT id FROM authors")['id'].tolist()
    
    if not author_ids:
        raise ValueError("No authors found. Generate authors first.")
    
    generated = 0
    
    for batch_num, current_batch_size in enumerate(
        DataGenerator.generate_in_batches(total_books, batch_size), 1
    ):
        # Use chunked author IDs to prevent memory issues
        author_chunk = random.choice(list(
            DataGenerator.chunk_list(author_ids, author_chunk_size)
        ))
        
        batch = _generate_books_batch(current_batch_size, author_chunk)
        books.add_book(batch)
        generated += len(batch)
        
        if batch_num % 100 == 0:
            print(f"Generated {generated:,}/{total_books:,} books...")
    
    print(f"✅ Generated {generated:,} books")


# ================ READERS ====================
def _generate_email(first_name: str, last_name: str) -> str:
    """Generate a unique email address"""
    return (f"{first_name.lower()}.{random.randint(1, 9999)}.{last_name.lower()}"
            f"{random.randint(1, 9999)}@{random.choice(DataGenerator.DOMAINS)}")


def _generate_readers_batch(batch_size: int) -> List[Dict]:
    """Generate a batch of reader data"""
    readers = []
    for _ in range(batch_size):
        first_name = fake.first_name()
        last_name = fake.last_name()
        readers.append({
            'first_name': first_name,
            'last_name': last_name,
            'email': _generate_email(first_name, last_name)
        })
    return readers


def generate_readers(
    total_readers: int = 5000,
    batch_size: Optional[int] = 1000
) -> None:
    """Generate readers in batches"""
    if batch_size is None:
        batch_size = min(1000, total_readers)
    
    generated = 0
    for batch_num, current_batch_size in enumerate(
        DataGenerator.generate_in_batches(total_readers, batch_size), 1
    ):
        batch = _generate_readers_batch(current_batch_size)
        readers.add_reader(batch)
        generated += len(batch)
    
    print(f"✅ Generated {generated} readers")


# ================ LOANS ====================
def _generate_loan_dates() -> Dict[str, Union[str, None]]:
    """Generate loan dates with return probability"""
    loan_date = fake.date_between(
        start_date=DataGenerator.LOAN_START_RANGE,
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


def _generate_loans_batch(
    batch_size: int,
    book_ids: List[int],
    reader_ids: List[int]
) -> List[Dict]:
    """Generate a batch of loan data"""
    loans = []
    for _ in range(batch_size):
        dates = _generate_loan_dates()
        loans.append({
            'book_id': random.choice(book_ids),
            'reader_id': random.choice(reader_ids),
            **dates
        })
    return loans


def generate_loans(
    total_loans: int = 1_000_000,
    batch_size: int = 50_000,
    book_limit: int = 1_000_000
) -> None:
    """Generate loans in batches with progress tracking"""
    
    print("Fetching book and reader IDs...")
    
    # Fetch data in chunks to prevent memory issues
    book_ids = db.sql_to_df(
        f"SELECT id FROM books LIMIT {book_limit}"
    )['id'].tolist()
    
    reader_ids = db.sql_to_df("SELECT id FROM readers")['id'].tolist()
    
    if not book_ids or not reader_ids:
        raise ValueError("Need books and readers before generating loans")
    
    generated = 0
    
    for batch_num, current_batch_size in enumerate(
        DataGenerator.generate_in_batches(total_loans, batch_size), 1
    ):
        batch = _generate_loans_batch(
            current_batch_size,
            book_ids,
            reader_ids
        )
        loan.add_loan(batch)
        generated += len(batch)
        
        if batch_num % 20 == 0:
            print(f"Generated {generated:,}/{total_loans:,} loans...")
    
    print(f"✅ Generated {generated:,} loans")


# ================ MAIN EXECUTION ====================
def generate_all_data(
    num_authors: int = 10000,
    num_books: int = 50_000_000,
    num_readers: int = 5000,
    num_loans: int = 1_000_000
) -> None:
    """Generate complete dataset"""
    print("Starting data generation...")
    
    # Generate in dependency order
    generate_authors(num_authors)
    generate_books(num_books)
    generate_readers(num_readers)
    generate_loans(num_loans)
    
    print("🎉 All data generated successfully!")


if __name__ == "__main__":
    # For smaller testing
    # generate_all_data(num_authors=100, num_books=1000, num_readers=100, num_loans=1000)
    
    # For full generation
    generate_all_data()