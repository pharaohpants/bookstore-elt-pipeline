{{ config(
    post_hook=[
      "alter table {{ this }} add constraint pk_fct_customer_repeat_order primary key (customer_repeat_order_sk)",
      "alter table {{ this }} add constraint fk_fct_customer_repeat_order_customer foreign key (customer_sk) references {{ ref('dim_customer') }} (customer_sk)"
    ]
) }}

with base as (
    select *
    from {{ ref('int_customer_repeat_order') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['cast(order_id as string)']) }} as customer_repeat_order_sk,
  b.customer_id,
  dc.customer_sk,
  b.order_id,
  b.order_date,
  b.previous_order_date,
  b.days_since_previous_order,
  b.previous_order_date is not null as is_repeat_order
from base as b
left join {{ ref('dim_customer') }} as dc
  on b.customer_id = dc.customer_id
  and b.order_date >= dc.valid_from
  and b.order_date < dc.valid_to
