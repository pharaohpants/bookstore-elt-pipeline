select
  cast(customer_id as integer) as customer_id,
  trim(first_name) as first_name,
  trim(last_name) as last_name,
  lower(trim(email)) as email,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'customer') }}

