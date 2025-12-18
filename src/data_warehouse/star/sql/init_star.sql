create table if not exists customer_dim (
    customer_id bigserial PRIMARY KEY,
    name varchar(255) not null,
    email varchar(255) not null,
    phone varchar(255) not null,
    start_date DATE not null,
    end_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT true
);

create table if not exists product_dim (
    product_id bigserial PRIMARY KEY,
    product_name varchar(255) not null,
    category varchar(255) not null,
    start_date DATE not null,
    end_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT true
);

create table if not exists date_dim (
    date_id bigserial primary key,
    date_value timestamp with time zone not null unique,
    year int not null generated always as (EXTRACT(YEAR FROM date_value AT TIME ZONE 'UTC')) stored,
    month int not null generated always as (EXTRACT(MONTH FROM date_value AT TIME ZONE 'UTC')) stored,
    day int not null generated always as (EXTRACT(DAY FROM date_value AT TIME ZONE 'UTC')) stored,
    hour int not null generated always as (EXTRACT(HOUR FROM date_value AT TIME ZONE 'UTC')) stored,
    minute int not null generated always as (EXTRACT(MINUTE FROM date_value AT TIME ZONE 'UTC')) stored,
    second int not null generated always as (EXTRACT(SECOND FROM date_value AT TIME ZONE 'UTC')) stored
);

create table if not exists sales_fact (
    sale_id bigserial PRIMARY KEY,
    customer_id bigint REFERENCES customer_dim(customer_id) not null,
    product_id bigint REFERENCES product_dim(product_id) not null,
    date_id bigint REFERENCES date_dim(date_id) not null,
    amount integer not null check (amount >= 1),
    quantity integer not null check (quantity >= 1)
);
