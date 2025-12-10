CREATE TABLE if not exists authors (
    id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    birth_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_product_region_date UNIQUE (first_name, last_name, birth_date)
);

CREATE TABLE if not exists books (
    id BIGSERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    author_id INTEGER NOT NULL,
    isbn VARCHAR(20) UNIQUE NOT NULL,
    publication_year INTEGER,
    genre VARCHAR(100),
    copies_available INTEGER DEFAULT 1 CHECK (copies_available >= 0),
    total_copies INTEGER DEFAULT 1 CHECK (total_copies >= 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
    CONSTRAINT unique_email UNIQUE (email)
);

CREATE TABLE if not exists loans (
    id BIGSERIAL PRIMARY KEY,
    book_id INTEGER NOT NULL,
    reader_id INTEGER NOT NULL,
    loan_date DATE DEFAULT CURRENT_DATE NOT NULL,
    due_date DATE NOT NULL,
    return_date DATE,
    FOREIGN KEY (book_id) REFERENCES books(id),
    FOREIGN KEY (reader_id) REFERENCES readers(id),
    CONSTRAINT unique_loan UNIQUE (reader_id, book_id)
);

ALTER TABLE if exists authors 
ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;

ALTER TABLE if exists books 
ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;

ALTER TABLE if exists readers 
ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;

ALTER TABLE if exists loans 
ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;