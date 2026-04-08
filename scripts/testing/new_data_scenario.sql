-- Scenario: New Data
-- Insert one new order with one order_line and one status history.

with selected_customer as (
    select c.customer_id
    from public.customer as c
    limit 1
),
selected_address as (
    select a.address_id
    from public.address as a
    limit 1
),
selected_shipping as (
    select sm.method_id
    from public.shipping_method as sm
    limit 1
),
created_order as (
    insert into public.cust_order (order_date, customer_id, shipping_method_id, dest_address_id)
    select
      now(),
      sc.customer_id,
      ss.method_id,
      sa.address_id
    from selected_customer as sc
    cross join selected_shipping as ss
    cross join selected_address as sa
    returning order_id
),
selected_book as (
    select b.book_id
    from public.book as b
    limit 1
),
inserted_line as (
    insert into public.order_line (order_id, book_id, price)
    select
      co.order_id,
      sb.book_id,
      cast(19.99 as numeric)
    from created_order as co
    cross join selected_book as sb
    returning order_id
)
insert into public.order_history (order_id, status_id, status_date)
select
  il.order_id,
  os.status_id,
  now()
from inserted_line as il
cross join lateral (
    select status_id
    from public.order_status
    order by status_id
    limit 1
) as os;
