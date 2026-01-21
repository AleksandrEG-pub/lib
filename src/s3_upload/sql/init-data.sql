INSERT INTO products (name, price)
SELECT 
    'Product ' || n,
    ROUND((RANDOM() * 100 + 5)::numeric, 2)
FROM generate_series(1, 5) AS n;


INSERT INTO orders (created_at)
SELECT 
    NOW() - (RANDOM() * INTERVAL '30 days')
FROM generate_series(1, 10);


INSERT INTO order_entry (order_id, product_id, quantity)
SELECT
    o.order_id,
    p.product_id,
    FLOOR(RANDOM() * 5 + 1)::int
FROM 
    orders o
CROSS JOIN 
    products p
ORDER BY RANDOM()
LIMIT 20;