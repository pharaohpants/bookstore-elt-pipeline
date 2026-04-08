select
  cast(author_id as integer) as author_id,
  trim(author_name) as author_name,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'author') }}

