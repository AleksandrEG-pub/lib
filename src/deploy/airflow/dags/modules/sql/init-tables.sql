CREATE TABLE IF NOT EXISTS raw_employees (
    employee_id BIGINT,
    name VARCHAR(100),
    age INTEGER,
    city VARCHAR(100),
    salary DECIMAL(10,2),
    loaded_at TIMESTAMP,
    record_source VARCHAR(100)
);


CREATE TABLE IF NOT EXISTS stage_employees (
    hash_key VARCHAR(100) PRIMARY KEY,
    employee_id BIGINT,
    name VARCHAR(100),
    age INTEGER,
    city VARCHAR(100),
    salary DECIMAL(10,2),
    loaded_at TIMESTAMP,
    record_source VARCHAR(100)
);
