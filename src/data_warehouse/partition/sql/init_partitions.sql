-- partitioned table
create table if not exists orders(
    order_id bigserial,
    price decimal(19, 2),
    load_date TIMESTAMP NOT null,
    PRIMARY KEY (order_id, load_date)
) PARTITION BY RANGE (load_date);

-- Partitions
CREATE TABLE IF NOT EXISTS orders_2023 PARTITION OF orders
    FOR VALUES FROM ('2023-01-01') TO ('2023-12-31');

CREATE TABLE IF NOT EXISTS orders_2024 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2024-12-31');

CREATE TABLE IF NOT EXISTS orders_2025 PARTITION OF orders
    FOR VALUES FROM ('2025-01-01') TO ('2025-12-31');

CREATE TABLE IF NOT EXISTS orders_2026 PARTITION OF orders
    FOR VALUES FROM ('2026-01-01') TO ('2026-12-31');

CREATE TABLE IF NOT EXISTS orders_2027 PARTITION OF orders
    FOR VALUES FROM ('2027-01-01') TO ('2027-12-31');

CREATE TABLE IF NOT EXISTS orders_default PARTITION OF orders DEFAULT;

-- Index
CREATE INDEX IF NOT EXISTS idx_orders_2024_load_date ON orders_2024(load_date);
CREATE INDEX IF NOT EXISTS idx_orders_2025_load_date ON orders_2025(load_date);
