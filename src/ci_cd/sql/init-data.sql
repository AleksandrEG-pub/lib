INSERT INTO customers_raw (customer_id) VALUES
(1001),
(1002);

INSERT INTO categories_raw (category_id) VALUES
(501),
(502);

INSERT INTO products_raw (product_id, category_id) VALUES
(2001, 501),
(2002, 502);

INSERT INTO stores_raw (store_id) VALUES
(301),
(302);

INSERT INTO orders_raw (order_id, customer_id, product_id, store_id) VALUES
(4001, 1001, 2001, 301),
(4002, 1002, 2002, 302);