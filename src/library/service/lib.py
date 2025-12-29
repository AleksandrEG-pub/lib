import logging
import library.persistence.database_setup as ds
import library.service.author as author
import library.service.reader as reader
import library.service.books as books
import library.service.loan as loan
import library.service.aggregations as aggs
import library.service.data_generation.library_data_generator as gen


def _create_update_opertaions():
    # add resources
    new_author = {
        'first_name': 'first_author',
        'last_name': 'last_author',
        'birth_date': '2000-12-12'
    }
    author.add_author(new_author)
    new_reader = {
        'first_name': 'first_name_reader',
        'last_name': 'last_name_reader',
        'email': 'reader@mail.com'
    }
    reader.add_reader(new_reader)
    new_book = {
        'title': 'title',
        'author_id': '1',
        'isbn': 1234567891234,
        'publication_year': 2000,
        'genre': 'genre',
        'copies_available': 1,
        'total_copies': 2
    }
    books.add_book(new_book)
    new_loan = {
        'book_id': '1',
        'reader_id': '1',
        'due_date': '2000-12-14'
    }
    loan.add_loan(new_loan)
    # update resource
    update_reader = {
        'first_name': 'updated',
        'last_name': 'updated'
    }
    reader.update_reader(1, update_reader)
    # delete resource
    books.delete_book(1)


def main():
    ds.setup_database()
    # optional generate data
    gen.generate_random_data()

    _create_update_opertaions()
    # get info operations
    books.get_all_books_with_authors(1, 10)

    # coalesce
    loan.get_loans_by_return_date()
    # union
    aggs.reader_author_union()
    # cte
    aggs.get_top_loaned_book_per_year_with_authors()
    # window
    aggs.get_count_loan_over_book()

    logging.info("all operations performed")
