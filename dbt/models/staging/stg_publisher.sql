select
  cast(publisher_id as integer) as publisher_id,
  trim(publisher_name) as publisher_name,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'publisher') }}

