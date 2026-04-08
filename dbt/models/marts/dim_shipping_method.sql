select
  {{ dbt_utils.generate_surrogate_key(['cast(method_id as string)']) }} as shipping_method_sk,
  method_id,
  method_name,
  shipping_cost
from {{ ref('stg_shipping_method') }}
