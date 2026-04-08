select
  cast(address_id as integer) as address_id,
  trim(street_number) as street_number,
  trim(street_name) as street_name,
  trim(city) as city,
  cast(country_id as integer) as country_id,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'address') }}

