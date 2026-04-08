select
  cast(language_id as integer) as language_id,
  trim(language_code) as language_code,
  trim(language_name) as language_name,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'book_language') }}

