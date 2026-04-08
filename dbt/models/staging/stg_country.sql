select
  cast(country_id as integer) as country_id,
  trim(country_name) as country_name,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'country') }}

