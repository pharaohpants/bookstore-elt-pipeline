-- Validation checks after pipeline run

-- 1) Initial load parity (source vs raw)
select 'source_customer' as table_name, count(*) as row_count from public.customer;

-- Run this in Snowflake:
-- select 'raw_customer' as table_name, count(*) as row_count
-- from <SNOWFLAKE_DATABASE>.<SNOWFLAKE_RAW_SCHEMA>.CUSTOMER;

-- 2) SCD Type 2 quality check in Snowflake:
-- exactly one current record per natural key
-- select customer_id
-- from <SNOWFLAKE_DATABASE>.<SNOWFLAKE_DW_SCHEMA>.DIM_CUSTOMER
-- where is_current
-- group by customer_id
-- having count(*) > 1;

-- 3) Daily aggregate sanity in Snowflake
-- select order_date_day, sum(total_sales_quantity) as qty
-- from <SNOWFLAKE_DATABASE>.<SNOWFLAKE_DW_SCHEMA>.FCT_DAILY_BOOK_SALES
-- group by order_date_day
-- order by order_date_day desc
-- limit 30;
