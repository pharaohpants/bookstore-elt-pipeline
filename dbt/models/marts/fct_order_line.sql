{{ config(
    post_hook=[
      "alter table {{ this }} add constraint pk_fct_order_line primary key (order_line_sk)",
      "alter table {{ this }} add constraint fk_fct_order_line_date foreign key (date_key) references {{ ref('dim_date') }} (date_key)",
      "alter table {{ this }} add constraint fk_fct_order_line_customer foreign key (customer_sk) references {{ ref('dim_customer') }} (customer_sk)",
      "alter table {{ this }} add constraint fk_fct_order_line_book foreign key (book_sk) references {{ ref('dim_book') }} (book_sk)",
      "alter table {{ this }} add constraint fk_fct_order_line_geography foreign key (geography_sk) references {{ ref('dim_geography') }} (geography_sk)",
      "alter table {{ this }} add constraint fk_fct_order_line_ship_method foreign key (shipping_method_sk) references {{ ref('dim_shipping_method') }} (shipping_method_sk)",
      "alter table {{ this }} add constraint fk_fct_order_line_status foreign key (order_status_sk) references {{ ref('dim_order_status') }} (order_status_sk)"
    ]
) }}

with base as (
    select *
    from {{ ref('int_order_line_enriched') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['cast(line_id as string)', 'cast(order_id as string)']) }} as order_line_sk,
  b.line_id,
  b.order_id,
  b.order_date,
  b.order_date_day,
  dd.date_key,
  coalesce(dc.customer_sk, 'UNKNOWN_CUSTOMER_SK') as customer_sk,
  coalesce(db.book_sk, 'UNKNOWN_BOOK_SK') as book_sk,
  dg.geography_sk,
  dsm.shipping_method_sk,
  dos.order_status_sk,
  b.customer_id,
  b.book_id,
  b.dest_address_id,
  b.shipping_method_id,
  b.order_status,
  b.quantity,
  b.unit_price,
  b.shipping_cost,
  b.gross_revenue,
  b.gross_revenue_incl_shipping
from base as b
left join {{ ref('dim_date') }} as dd
  on b.order_date_day = dd.date_day
left join {{ ref('dim_customer') }} as dc
  on b.customer_id = dc.customer_id
  and b.order_date >= dc.valid_from
  and b.order_date < dc.valid_to
left join {{ ref('dim_book') }} as db
  on b.book_id = db.book_id
  and b.order_date >= db.valid_from
  and b.order_date < db.valid_to
left join {{ ref('dim_geography') }} as dg
  on b.dest_address_id = dg.address_id
left join {{ ref('dim_shipping_method') }} as dsm
  on b.shipping_method_id = dsm.method_id
left join {{ ref('dim_order_status') }} as dos
  on b.order_status = dos.status_value
