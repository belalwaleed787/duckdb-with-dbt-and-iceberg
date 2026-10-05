{{ config(materialized='iceberg_table') }}

-- Daily totals per pickup zone, for dashboards that do not need trip-level detail.
select
    pickup_date_key                                         as date_key,
    pickup_location_id                                      as location_id,
    count(*)                                                as trip_count,
    sum(passenger_count)::bigint                            as passenger_count,
    round(sum(trip_distance_miles), 2)                      as trip_distance_miles,
    sum(fare_amount)                                        as fare_amount,
    sum(tip_amount)                                         as tip_amount,
    sum(total_amount)                                       as total_amount,
    round(avg(trip_distance_miles), 2)                      as avg_trip_distance_miles,
    round(avg(trip_duration_minutes::double), 2)            as avg_trip_duration_minutes,
    round(avg(total_amount::double), 2)                     as avg_total_amount,
    count(*) filter (where payment_type_id = 1)             as card_trip_count,
    count(*) filter (where payment_type_id = 2)             as cash_trip_count
from {{ ref('fact_trips') }}
group by all
