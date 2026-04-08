with ordered as (
    select
      customer_id,
      order_id,
      order_date,
      lag(order_date) over (
        partition by customer_id
        order by order_date
      ) as previous_order_date
    from {{ ref('stg_cust_order') }}
)

select
  customer_id,
  order_id,
  order_date,
  previous_order_date,
  datediff(day, previous_order_date, order_date) as days_since_previous_order
from ordered
