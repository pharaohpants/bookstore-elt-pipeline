{% snapshot book_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='book_id',
    strategy='check',
    check_cols=['title', 'isbn13', 'language_id', 'num_pages', 'publication_date', 'publisher_id'],
    invalidate_hard_deletes=True
  )
}}

select
  book_id,
  title,
  isbn13,
  language_id,
  num_pages,
  publication_date,
  publisher_id
from {{ ref('stg_book') }}

{% endsnapshot %}
