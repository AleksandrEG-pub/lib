INSERT INTO customers (customer_id) VALUES
(1001),
(1002);

INSERT INTO categories (category_id) VALUES
(501),
(502);

INSERT INTO products (product_id, category_id) VALUES
(2001, 501),
(2002, 502);

INSERT INTO stores (store_id) VALUES
(301),
(302);

INSERT INTO orders (order_id, customer_id, product_id, store_id) VALUES
(4001, 1001, 2001, 301),
(4002, 1002, 2002, 302);