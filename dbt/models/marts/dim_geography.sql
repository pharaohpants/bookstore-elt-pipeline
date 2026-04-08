select
  {{ dbt_utils.generate_surrogate_key(['cast(a.address_id as string)']) }} as geography_sk,
  a.address_id,
  a.city,
  a.country_id,
  c.country_name
from {{ ref('stg_address') }} as a
left join {{ ref('stg_country') }} as c
  on a.country_id = c.country_id
