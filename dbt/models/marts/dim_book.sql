with src as (
    select
      book_id,
      title,
      isbn13,
      language_id,
      num_pages,
      publication_date,
      publisher_id,
      dbt_valid_from,
      dbt_valid_to
    from {{ ref('book_snapshot') }}
)

select
  {{ dbt_utils.generate_surrogate_key(["cast(src.book_id as string)", "cast(src.dbt_valid_from as string)"]) }} as book_sk,
  src.book_id,
  src.title,
  src.isbn13,
  src.language_id,
  lang.language_name,
  src.num_pages,
  src.publication_date,
  src.publisher_id,
  pub.publisher_name,
  src.dbt_valid_from as valid_from,
  coalesce(src.dbt_valid_to, to_timestamp_ntz('9999-12-31 00:00:00')) as valid_to,
  src.dbt_valid_to is null as is_current
from src
left join {{ ref('stg_publisher') }} as pub
  on src.publisher_id = pub.publisher_id
left join {{ ref('stg_book_language') }} as lang
  on src.language_id = lang.language_id
