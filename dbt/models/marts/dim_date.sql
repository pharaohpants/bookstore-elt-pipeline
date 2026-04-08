with calendar as (
    select
      dateadd(day, seq4(), to_date('2000-01-01')) as date_day
    from table(generator(rowcount => 36525))
)

select
  cast(to_char(date_day, 'YYYYMMDD') as integer) as date_key,
  cast(date_day as date) as date_day,
  year(date_day) as year,
  quarter(date_day) as quarter,
  month(date_day) as month,
  trim(to_char(date_day, 'Month')) as month_name,
  day(date_day) as day,
  dayofweekiso(date_day) as day_of_week,
  trim(to_char(date_day, 'Day')) as day_name,
  case when dayofweekiso(date_day) in (6, 7) then true else false end as is_weekend
from calendar
where date_day <= current_date()

