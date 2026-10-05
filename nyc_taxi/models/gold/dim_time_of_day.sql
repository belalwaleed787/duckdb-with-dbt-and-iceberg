{{ config(materialized='iceberg_table') }}

-- One row per hour of the day.
with hours as (
    select range::integer as hour_of_day
    from range(24)
)

select
    hour_of_day,
    lpad(hour_of_day::varchar, 2, '0') || ':00-' || lpad(hour_of_day::varchar, 2, '0') || ':59' as hour_label,
    case
        when hour_of_day between 0 and 5 then 'Late night'
        when hour_of_day between 6 and 9 then 'Morning rush'
        when hour_of_day between 10 and 15 then 'Midday'
        when hour_of_day between 16 and 19 then 'Evening rush'
        else 'Evening'
    end as day_part,
    hour_of_day between 6 and 9 or hour_of_day between 16 and 19 as is_rush_hour
from hours
