INSERT INTO customers (customer_id, name, email, phone) VALUES
(1, 'Alice Smith', 'alice@example.com', '+1-555-1001'),
(2, 'Bob Johnson', 'bob@example.com', '+1-555-1002');

INSERT INTO products (product_id, product_name, category) VALUES
(101, 'Laptop', 'Electronics'),
(102, 'Office Chair', 'Furniture');

INSERT INTO sales (sale_id, customer_id, product_id, quantity, amount, sale_date) VALUES
(1001, 1, 101, 1, 1200, '2025-01-10 10:15:00+00'),
(1002, 2, 102, 2, 300,  '2025-01-11 14:20:00+00');