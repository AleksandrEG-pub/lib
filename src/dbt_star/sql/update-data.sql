INSERT INTO customers (customer_id, name, email, phone) VALUES
(3, 'Monica Right', 'monica@example.com', '+1-555-1003');

INSERT INTO products (product_id, product_name, category) VALUES
(103, 'Headphones', 'Electronics');

update customers
set name = 'Alice Smith Update'
where customer_id = 1;

update products
set product_name = 'Laptop Update Update'
where product_id = 101;

INSERT INTO sales (sale_id, customer_id, product_id, quantity, amount, sale_date) VALUES
(1003, 2, 103, 1, 250, '2025-01-18 10:30:00+00'),
(1004, 3, 102, 2, 300, '2025-01-19 14:50:00+00');