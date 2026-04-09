{{ config(
    post_hook=[
      "alter table {{ this }} add constraint pk_dim_shipping_method primary key (shipping_method_sk)"
    ]
) }}

select
  {{ dbt_utils.generate_surrogate_key(['cast(method_id as string)']) }} as shipping_method_sk,
  method_id,
  method_name,
  shipping_cost
from {{ ref('stg_shipping_method') }}
