select
  cast(book_id as integer) as book_id,
  cast(author_id as integer) as author_id,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'book_author') }}

