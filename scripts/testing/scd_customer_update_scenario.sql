-- Scenario: SCD Type 2
-- Update one customer attribute to verify dbt snapshot creates a new version.

update public.customer
set
  first_name = concat(first_name, '_updated')
where customer_id = (
    select customer_id
    from public.customer
    order by customer_id
    limit 1
);
