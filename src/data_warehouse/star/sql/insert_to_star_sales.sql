insert into sales_fact (
  sale_hash,
  sale_id,
  customer_sk,
  product_sk,
  quantity,
  amount,
  date_value
)
select
  md5(
    sp.sale_id::text || '|' ||
    sp.product_id::text || '|' ||
    sp.customer_id::text || '|' ||
    sp.date_value::text || '|' ||
    sp.amount::text || '|' ||
    sp.quantity::text
  ) as sale_hash,
  sp.sale_id,
  cd.customer_sk,
  pd.product_sk,
  sp.quantity,
  sp.amount,
  sp.date_value
from sales_pre sp
join product_dim pd
  on sp.product_id = pd.product_id
 and pd.is_current = true
join customer_dim cd
  on sp.customer_id = cd.customer_id
 and cd.is_current = true
where not exists (
  select 1
  from sales_fact sf
  where sf.sale_hash = md5(
      sp.sale_id::text || '|' ||
      sp.product_id::text || '|' ||
      sp.customer_id::text || '|' ||
      sp.date_value::text || '|' ||
      sp.amount::text || '|' ||
      sp.quantity::text
  )
);
