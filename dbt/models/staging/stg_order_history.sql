select
  cast(history_id as integer) as history_id,
  cast(order_id as integer) as order_id,
  cast(status_id as integer) as status_id,
  cast(status_date as timestamp) as status_date,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'order_history') }}

