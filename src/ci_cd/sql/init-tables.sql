CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT
);

CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT,
    category_id BIGINT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT,
    customer_id BIGINT,
    product_id BIGINT,
    store_id BIGINT
);

CREATE TABLE IF NOT EXISTS stores (
    store_id BIGINT
);

CREATE TABLE IF NOT EXISTS categories (
    category_id BIGINT
);