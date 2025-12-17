CREATE TABLE if not exists authors (
    id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    birth_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL,
    CONSTRAINT unique_author_name_birth UNIQUE (first_name, last_name, birth_date)
);

CREATE TABLE if not exists books (
    id BIGSERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    author_id BIGINT NOT NULL,
    isbn VARCHAR(20) UNIQUE NOT NULL,
    publication_year INTEGER,
    genre VARCHAR(100),
    copies_available INTEGER DEFAULT 1 CHECK (copies_available >= 0),
    total_copies INTEGER DEFAULT 1 CHECK (total_copies >= 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE CASCADE,
    CONSTRAINT unique_isbn UNIQUE (isbn),
    CONSTRAINT unique_title_author UNIQUE (author_id, title, publication_year)
);

CREATE TABLE if not exists readers (
    id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    registration_date DATE DEFAULT CURRENT_DATE,
    deleted_at TIMESTAMP DEFAULT NULL,
    CONSTRAINT unique_email UNIQUE (email)
);

CREATE TABLE if not exists loans (
    id BIGSERIAL PRIMARY KEY,
    book_id BIGINT NOT NULL,
    reader_id BIGINT NOT NULL,
    loan_date DATE DEFAULT CURRENT_DATE NOT NULL,
    due_date DATE NOT NULL,
    return_date DATE,
    deleted_at TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (book_id) REFERENCES books(id),
    FOREIGN KEY (reader_id) REFERENCES readers(id),
    CONSTRAINT unique_loan UNIQUE (reader_id, book_id)
);

-- for top N aggregate
CREATE INDEX if not exists idx_books_publication_year ON books(publication_year);
-- for author join
CREATE INDEX if not exists idx_books_author_id ON books(author_id);
-- for patitioning by book_id
CREATE INDEX if not exists idx_loans_book_id ON loans(book_id);

CREATE INDEX if not exists idx_authors_deleted_at ON authors(deleted_at);
CREATE INDEX if not exists idx_books_deleted_at ON books(deleted_at);
CREATE INDEX if not exists idx_readers_deleted_at ON readers(deleted_at);
CREATE INDEX if not exists idx_loans_deleted_at ON loans(deleted_at);
