create table if not exists customers (
    customer_id bigint PRIMARY KEY,
    name varchar(255) not null,
    email varchar(255) not null,
    phone varchar(255) not null
);

create table if not exists products (
    product_id bigint PRIMARY KEY,
    product_name varchar(255) not null,
    category varchar(255) not null
);

create table if not exists sales (
    sale_id bigint PRIMARY KEY,
    customer_id bigint,
    product_id bigint,
    quantity integer not null check (quantity >= 1),
    amount integer not null check (amount >= 1),
    sale_date timestamp with time zone not null,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
