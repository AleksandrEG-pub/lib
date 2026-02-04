{{ config(materialized='incremental') }}

select
    s.sale_id,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.amount,
    s.sale_date
from {{ source('postgres', 'sales') }} s

{% if is_incremental() %}
where s.sale_id not in (select sale_id from {{ this }})
{% endif %}
