create table if not exists customer_dim (
    customer_hash varchar(255) unique,
    customer_id bigint,
    customer_sk bigserial PRIMARY KEY,
    name varchar(255) not null,
    email varchar(255) not null,
    phone varchar(255) not null,
    start_date DATE not null,
    end_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT true
);

create table if not exists product_dim (
    product_hash varchar(255) unique,
    product_id bigint,
    product_sk bigserial PRIMARY KEY,
    product_name varchar(255) not null,
    category varchar(255) not null,
    start_date DATE not null,
    end_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT true
);

create table if not exists sales_fact (
    sale_hash varchar(255) unique,
    sale_id bigint,
    customer_sk bigint,
    product_sk bigint,
    quantity integer not null check (quantity >= 1),
    amount integer not null check (amount >= 1),
    date_value timestamp with time zone not null,
    FOREIGN KEY (customer_sk) REFERENCES customer_dim(customer_sk),
    FOREIGN KEY (product_sk) REFERENCES product_dim(product_sk)
);
