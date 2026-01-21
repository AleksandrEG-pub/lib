insert into sales_fact (
  sale_id,
  customer_sk,
  product_sk,
  quantity,
  amount,
  date_value
)
select
  sp.sale_id,
  pd.product_sk,
  cd.customer_sk,
  sp.amount,
  sp.quantity,
  sp.date_value
from sales_pre sp
join product_dim pd
  on sp.product_id = pd.product_id
 and pd.is_current = true
join customer_dim cd
  on sp.customer_id = cd.customer_id
 and cd.is_current = true;
