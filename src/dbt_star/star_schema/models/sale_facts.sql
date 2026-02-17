{{ config(materialized='incremental') }}
with source_sales as (
    select
        s.sale_id,
        s.sale_date,
        s.customer_id,
        s.product_id,
        s.quantity,
        s.amount,
        md5(
            cast(s.sale_id     as text) || '|' ||
            cast(s.sale_date   as text) || '|' ||
            cast(s.customer_id as text) || '|' ||
            cast(s.product_id  as text) || '|' ||
            cast(s.quantity    as text) || '|' ||
            cast(s.amount      as text)
        ) as sale_hash
    from {{ source('postgres', 'sales') }} s
)
select
    ss.sale_id,
    ss.sale_date,
    ss.quantity,
    ss.amount,
    dc.customer_sk,
    dp.product_sk,
    ss.sale_hash
from source_sales ss
join {{ ref('dim_customer') }} dc
  on ss.customer_id = dc.customer_id
join {{ ref('dim_product') }} dp
  on ss.product_id = dp.product_id
{% if is_incremental() %}
where ss.sale_hash not in (select sale_hash from {{ this }})
{% endif %}
