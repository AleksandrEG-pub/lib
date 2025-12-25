import library.service.author as author
import library.service.reader as readers
import library.service.books as books
import library.service.loan as loan
import library.service.data_generation.loan_generation as loan_generation
import library.service.data_generation.book_generation as book_generation
import library.service.data_generation.author_generation as author_generation
import library.service.data_generation.reader_generation as reader_generation
import database.data_generator as data_gen

def batch_size(total: int) -> int:
    if total <= 1000:
        return total
    else:
        return 1000

def generate_random_data(
    num_authors: int = 100,
    num_books: int = 10_000,
    num_readers: int = 100,
    num_loans: int = 200
) -> None:
    """Generate complete dataset"""
    print("Starting data generation...")
    # ================ AUTHORS ====================
    data_gen.generate_batches(
        total_items=num_authors,
        batch_size=batch_size(num_authors),
        create_item_function=author_generation.make_author,
        after_batch_function=author.add_author,
    )
    # ================ BOOKS ====================
    make_book = book_generation.make_book_factory()
    data_gen.generate_batches(
        total_items=num_books,
        batch_size=batch_size(num_books),
        create_item_function=make_book,
        after_batch_function=books.add_book,
    )
    # ================ READERS ====================
    data_gen.generate_batches(
        total_items=num_readers,
        batch_size=batch_size(num_readers),
        create_item_function=reader_generation.make_reader,
        after_batch_function=readers.add_reader,
    )
    # ================ LOANS ====================
    make_loan = loan_generation.make_loan_factory()
    data_gen.generate_batches(
        total_items=num_loans,
        batch_size=batch_size(num_loans),
        create_item_function=make_loan,
        after_batch_function=loan.add_loan,
    )
    print("🎉 All data generated successfully!")
