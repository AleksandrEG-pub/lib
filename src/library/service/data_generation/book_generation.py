import random
from typing import Callable, Dict, List
from library.persistence.database_connection import db
import database.data_generator as generate_random_data

from faker import Faker
fake = Faker()


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


def _rnd_author_id_generator():
    author_ids = db.sql_to_df("SELECT id FROM authors")['id'].tolist()
    if not author_ids:
        raise ValueError("No authors found. Generate authors first.")

    def get_next():
        return random.choice(author_ids)
    return get_next


def make_book_factory() -> Callable[[None], Dict]:
    author_gen = _rnd_author_id_generator()
    isbn_gen = generate_random_data.create_isbn_generator()
    GENRES = ['Fiction', 'Non-Fiction', 'Science', 'Fantasy', 'Mystery',
              'Biography', 'History', 'Romance', 'Technology', 'Children']
    total_copies = random.randint(1, 10)
    copies_available = random.randint(1, total_copies)

    def make_book() -> Dict:
        return {
            'title': generate_random_data.rnd_title(),
            'author_id': author_gen(),
            'isbn': isbn_gen(),
            'publication_year': random.randint(1950, 2023),
            'genre': random.choice(GENRES),
            'copies_available': copies_available,
            'total_copies': total_copies
        }
    return make_book
