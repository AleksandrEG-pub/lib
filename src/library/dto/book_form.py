from dataclasses import dataclass


@dataclass
class BookForm:
    title: str
    author_id: str
    isbn: str
    publication_year: str
    genre: str
    copies_available: str
    total_copies: str
