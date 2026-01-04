create table if not exists orders (
    order_id bigserial primary key,
    created_at timestamp default now()
);

create table if not exists products (
    product_id bigserial primary key,
    name varchar(255),
    price decimal(13,2),
    created_at timestamp default now()
);

create table if not exists order_entry (
    order_entry_id bigserial primary key,
    order_id bigint references orders(order_id),
    product_id bigint references products(product_id),
    quantity int,
    created_at timestamp default now()
);
