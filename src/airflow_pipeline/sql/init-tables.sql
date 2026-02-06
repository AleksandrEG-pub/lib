CREATE TABLE IF NOT EXISTS bakery_deliveries (
    delivery_id VARCHAR(10),
    item_type VARCHAR(50),
    quantity INTEGER,
    price_per_unit DECIMAL(5,2),
    manufacture_datetime TIMESTAMP,
    source_file VARCHAR(100),
    upload_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_quality_checks (
    check_id BIGSERIAL,
    file_name VARCHAR(100),
    is_valid boolean,
    check_timestamp TIMESTAMP default current_timestamp
);