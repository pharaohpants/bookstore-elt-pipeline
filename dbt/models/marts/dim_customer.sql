{{ config(
    post_hook=[
      "alter table {{ this }} add constraint pk_dim_customer primary key (customer_sk)"
    ]
) }}

with src as (
    select
      customer_id,
      first_name,
      last_name,
      email,
      dbt_valid_from,
      dbt_valid_to
    from {{ ref('customer_snapshot') }}
)

select
  {{ dbt_utils.generate_surrogate_key(["cast(customer_id as string)", "cast(dbt_valid_from as string)"]) }} as customer_sk,
  customer_id,
  first_name,
  last_name,
  email,
  dbt_valid_from as valid_from,
  coalesce(dbt_valid_to, to_timestamp_ntz('9999-12-31 00:00:00')) as valid_to,
  dbt_valid_to is null as is_current
from src
