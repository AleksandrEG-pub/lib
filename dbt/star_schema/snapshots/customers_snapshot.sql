{% snapshot customers_snapshot %}

{{
    config(
      target_schema='public',
      strategy='check',
      unique_key='customer_id',
      check_cols=['name', 'email', 'phone']
    )
}}

select
    customer_id,
    name,
    email,
    phone
from {{ source('postgres', 'customers') }}v

{% endsnapshot %}
