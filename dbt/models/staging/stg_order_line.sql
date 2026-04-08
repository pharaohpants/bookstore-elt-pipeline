select
  cast(line_id as integer) as line_id,
  cast(order_id as integer) as order_id,
  cast(book_id as integer) as book_id,
  cast(price as numeric) as unit_price,
  1 as quantity,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'order_line') }}

