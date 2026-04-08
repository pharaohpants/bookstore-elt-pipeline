with latest_status as (
    select
      oh.order_id,
      os.status_value as order_status,
      row_number() over (
        partition by oh.order_id
        order by oh.status_date desc, oh.history_id desc
      ) as rn
    from {{ ref('stg_order_history') }} as oh
    left join {{ ref('stg_order_status') }} as os
      on oh.status_id = os.status_id
)

select
  ol.line_id,
  o.order_id,
  o.order_date,
  o.order_date_day,
  o.customer_id,
  ol.book_id,
  o.shipping_method_id,
  o.dest_address_id,
  ol.quantity,
  ol.unit_price,
  cast(ol.quantity * ol.unit_price as numeric) as gross_revenue,
  coalesce(sm.shipping_cost, cast(0 as numeric)) as shipping_cost,
  cast((ol.quantity * ol.unit_price) + coalesce(sm.shipping_cost, cast(0 as numeric)) as numeric) as gross_revenue_incl_shipping,
  coalesce(ls.order_status, 'UNKNOWN') as order_status,
  adr.city,
  ctr.country_name
from {{ ref('stg_order_line') }} as ol
inner join {{ ref('stg_cust_order') }} as o
  on ol.order_id = o.order_id
left join {{ ref('stg_shipping_method') }} as sm
  on o.shipping_method_id = sm.method_id
left join latest_status as ls
  on o.order_id = ls.order_id
  and ls.rn = 1
left join {{ ref('stg_address') }} as adr
  on o.dest_address_id = adr.address_id
left join {{ ref('stg_country') }} as ctr
  on adr.country_id = ctr.country_id
