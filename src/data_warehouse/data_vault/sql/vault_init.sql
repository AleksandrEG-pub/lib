/* 
DV2
record_source varchar(255) NOT NULL
load_date TIMESTAMP NOT NULL

SCD
start_date DATE
end_date DATE
is_current BOOLEAN
 */

-- HUBS
create table if not exists hub_customers (
    customer_id varchar(255) UNIQUE NOT NULL,
    customer_hashkey varchar(255) PRIMARY KEY,
    record_source varchar(255) NOT NULL,
    load_date TIMESTAMP NOT NULL
);

create table if not exists hub_products (
    product_id varchar(255) UNIQUE NOT NULL,
    product_hashkey varchar(255) PRIMARY KEY,
    record_source varchar(255) NOT NULL,
    load_date TIMESTAMP NOT NULL
);

create table if not exists hub_orders (
    order_id varchar(255) UNIQUE NOT NULL,
    order_hashkey varchar(255) PRIMARY KEY,
    record_source varchar(255) NOT NULL,
    load_date TIMESTAMP NOT NULL
);

create table if not exists hub_order_statuses (
    order_status_id varchar(255) UNIQUE NOT NULL,
    order_status_hashkey varchar(255) PRIMARY KEY,
    record_source varchar(255) NOT NULL,
    load_date TIMESTAMP NOT NULL
);


-- Satellites
create table if not exists sat_customers (
    customer_hashkey varchar(255) NOT NULL REFERENCES hub_customers(customer_hashkey),
    name varchar(255) NOT NULL,
    record_source varchar(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    load_end_date TIMESTAMP,
    PRIMARY KEY (customer_hashkey, load_date)
);

create table if not exists sat_products (
    product_hashkey varchar(255) NOT NULL REFERENCES hub_products(product_hashkey),
    name varchar(255) NOT NULL,
    record_source varchar(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    load_end_date TIMESTAMP,
    PRIMARY KEY (product_hashkey, load_date)
);

create table if not exists sat_orders (
    order_hashkey varchar(255) NOT NULL REFERENCES hub_orders(order_hashkey),
    type varchar(255) NOT NULL,
    record_source varchar(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    load_end_date TIMESTAMP,
    PRIMARY KEY (order_hashkey, load_date)
);

create table if not exists sat_order_statuses (
    order_status_hashkey varchar(255) NOT NULL REFERENCES hub_order_statuses(order_status_hashkey),
    status varchar(255) NOT NULL,
    record_source varchar(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    load_end_date TIMESTAMP,
    PRIMARY KEY (order_status_hashkey, load_date)
);

-- Links
create table if not exists link_customer_order (
    customer_order_hash varchar(255) PRIMARY KEY,
    order_hashkey varchar(255) NOT NULL REFERENCES hub_orders(order_hashkey),
    customer_hashkey varchar(255) NOT NULL REFERENCES hub_customers(customer_hashkey),
    record_source varchar(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    CONSTRAINT order_hash_customer_hash_uniquekey UNIQUE (order_hashkey, customer_hashkey)
);

create table if not exists link_order_product (
    order_product_hash varchar(255) PRIMARY KEY, 
    order_hashkey varchar(255) NOT NULL REFERENCES hub_orders(order_hashkey),
    product_hashkey varchar(255) NOT NULL REFERENCES hub_products(product_hashkey),
    record_source varchar(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    CONSTRAINT order_hash_product_hash_uniquekey UNIQUE (order_hashkey, product_hashkey)
);  

create table if not exists link_order_order_status (
    order_order_status_hash varchar(255) PRIMARY KEY,
    order_hashkey varchar(255) NOT NULL REFERENCES hub_orders(order_hashkey),
    order_status_hashkey varchar(255) NOT NULL REFERENCES hub_order_statuses(order_status_hashkey),
    record_source varchar(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    CONSTRAINT order_hash_order_status_hash_uniquekey UNIQUE (order_hashkey, order_status_hashkey)
);
