select
  cast(to_char(order_date_day, 'YYYYMMDD') as integer) as date_key,
  order_date_day,
  book_sk,
  book_id,
  count(distinct order_id) as total_orders,
  sum(quantity) as total_sales_quantity,
  sum(gross_revenue) as total_gross_revenue,
  sum(gross_revenue_incl_shipping) as total_gross_revenue_incl_shipping
from {{ ref('fct_order_line') }}
group by 1, 2, 3, 4

