select
  {{ dbt_utils.generate_surrogate_key(['cast(status_id as string)']) }} as order_status_sk,
  status_id,
  status_value
from {{ ref('stg_order_status') }}
