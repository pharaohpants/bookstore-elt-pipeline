select
  cast(method_id as integer) as method_id,
  trim(method_name) as method_name,
  cast(cost as numeric) as shipping_cost,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'shipping_method') }}

