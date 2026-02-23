CREATE TABLE IF NOT EXISTS raw_employees (
    employee_id BIGINT,
    name VARCHAR(100),
    age VARCHAR(5),
    city VARCHAR(100),
    salary VARCHAR(15),
    loaded_at TIMESTAMP NOT NULL,
    record_source VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS stage_employees (
    hash_key VARCHAR(100) PRIMARY KEY,
    employee_id BIGINT,
    name VARCHAR(100),
    age INTEGER,
    city VARCHAR(100),
    salary DECIMAL(10,2),
    loaded_at TIMESTAMP NOT NULL,
    record_source VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS data_quality_checks (
    check_id PRIMARY KEY BIGSERIAL,
    check_name VARCHAR(50),
    status VARCHAR(50),
    value VARCHAR(100),
    timestamp TIMESTAMP default current_timestamp
);

CREATE INDEX idx_loaded_at ON raw_employees(loaded_at DESC);
CREATE INDEX idx_created_at ON data_quality_checks(timestamp DESC);
