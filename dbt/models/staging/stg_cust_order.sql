select
  cast(order_id as integer) as order_id,
  cast(order_date as timestamp) as order_date,
  cast(date(order_date) as date) as order_date_day,
  cast(customer_id as integer) as customer_id,
  cast(shipping_method_id as integer) as shipping_method_id,
  cast(dest_address_id as integer) as dest_address_id,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'cust_order') }}

