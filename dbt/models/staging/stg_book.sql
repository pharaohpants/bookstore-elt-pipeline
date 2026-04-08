select
  cast(book_id as integer) as book_id,
  trim(title) as title,
  trim(isbn13) as isbn13,
  cast(language_id as integer) as language_id,
  cast(num_pages as integer) as num_pages,
  cast(publication_date as date) as publication_date,
  cast(publisher_id as integer) as publisher_id,
  cast(_elt_loaded_at as timestamp) as _elt_loaded_at
from {{ source('pacbook_raw', 'book') }}

