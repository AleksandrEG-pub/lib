-- seq scan orders_2023 - few data, no index
-- explain
select *
from orders o
where o.load_date between '2023-03-03' AND '2023-04-03';

-- index scan idx_orders_2024_load_date - more data, index exist
-- explain
select *
from orders o
where o.load_date between '2024-03-03' AND '2024-04-03';

-- index scan idx_orders_2025_load_date - most data, index exist
-- explain
select *
from orders o
where o.load_date between '2025-03-03' AND '2025-04-03';

-- seq scan by orders.default table
-- explain
select *
from orders o
where o.load_date between '2028-03-03' AND '2028-04-03';