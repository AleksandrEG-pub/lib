create table if not exists sales_pre (
    sale_id bigint,
    customer_id bigint,
    product_id bigint,
    amount decimal(13,2),
    quantity integer,
    date_value timestamp with time zone
);
