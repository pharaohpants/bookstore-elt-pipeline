select
  cast(status_id as integer) as status_id,
  trim(status_value) as status_value,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'order_status') }}

