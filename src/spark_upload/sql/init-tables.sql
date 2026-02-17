create table if not exists products (
    product_id bigserial primary key,
    name varchar(255),
    price decimal(13,2),
    created_at timestamp
);

