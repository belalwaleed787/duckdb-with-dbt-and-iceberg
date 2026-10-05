{{ config(materialized='iceberg_table') }}

-- One row per day, covering every full calendar year that has trips.
with bounds as (
    select
        date_trunc('year', min(pickup_at))::date                                    as first_day,
        (date_trunc('year', max(dropoff_at)) + interval 1 year - interval 1 day)::date as last_day
    from {{ ref('silver_yellow_trips') }}
),

days as (
    select unnest(generate_series(first_day::timestamp, last_day::timestamp, interval 1 day))::date as calendar_date
    from bounds
)

select
    strftime(calendar_date, '%Y%m%d')::integer  as date_key,
    calendar_date,
    year(calendar_date)::integer                as year,
    quarter(calendar_date)::integer             as quarter,
    month(calendar_date)::integer               as month,
    monthname(calendar_date)                    as month_name,
    strftime(calendar_date, '%Y-%m')            as year_month,
    day(calendar_date)::integer                 as day_of_month,
    isodow(calendar_date)::integer              as day_of_week,  -- 1 = Monday
    dayname(calendar_date)                      as day_name,
    weekofyear(calendar_date)::integer          as iso_week,
    isodow(calendar_date) in (6, 7)             as is_weekend
from days
