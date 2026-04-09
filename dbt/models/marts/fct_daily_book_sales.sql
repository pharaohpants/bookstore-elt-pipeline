{{ config(
    post_hook=[
      "alter table {{ this }} add constraint pk_fct_daily_book_sales primary key (daily_book_sales_sk)",
      "alter table {{ this }} add constraint fk_fct_daily_book_sales_date foreign key (date_key) references {{ ref('dim_date') }} (date_key)",
      "alter table {{ this }} add constraint fk_fct_daily_book_sales_book foreign key (book_sk) references {{ ref('dim_book') }} (book_sk)"
    ]
) }}

with aggregated as (
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
)

select
  {{ dbt_utils.generate_surrogate_key(['cast(date_key as string)', 'cast(book_sk as string)', 'cast(book_id as string)']) }} as daily_book_sales_sk,
  date_key,
  order_date_day,
  book_sk,
  book_id,
  total_orders,
  total_sales_quantity,
  total_gross_revenue,
  total_gross_revenue_incl_shipping
from aggregated

