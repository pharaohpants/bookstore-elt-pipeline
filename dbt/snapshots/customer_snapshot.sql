{% snapshot customer_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['first_name', 'last_name', 'email'],
    invalidate_hard_deletes=True
  )
}}

select
  customer_id,
  first_name,
  last_name,
  email
from {{ ref('stg_customer') }}

{% endsnapshot %}
