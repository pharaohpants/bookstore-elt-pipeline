{{ config(
    post_hook=[
      "alter table {{ this }} add constraint pk_dim_order_status primary key (order_status_sk)"
    ]
) }}

select
  {{ dbt_utils.generate_surrogate_key(['cast(status_id as string)']) }} as order_status_sk,
  status_id,
  status_value
from {{ ref('stg_order_status') }}
