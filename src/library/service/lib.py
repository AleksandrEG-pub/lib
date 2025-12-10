import logging
import library.persistence.database_setup as ds
import library.service.author as author
import library.service.reader as reader
import library.service.books as books
import library.service.data_generator as gen


def main():
    ds.setup_database()
    # gen.generate_authors(100000, 1000)
    # gen.generate_readers(100000, 1000)
    # gen.generate_books(4, 2)
    # gen.generate_books()
    # gen.generate_loans(1000000, 10000)
    
    # update_reader = {
        # 'first_name': 'updated',
        # 'last_name': 'updated'
    # }
    # reader.update_reader(1, update_reader)
    
    books.delete_book(8692603)
    

    # author.add_author(auth)
