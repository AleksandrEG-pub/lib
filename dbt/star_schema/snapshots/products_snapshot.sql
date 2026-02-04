{% snapshot products_snapshot %}

{{
    config(
      target_schema='public',
      strategy='check',
      unique_key='product_id',
      check_cols=['product_name', 'category']
    )
}}

select
    product_id,
    product_name,
    category
from {{ source('postgres', 'products') }}

{% endsnapshot %}
