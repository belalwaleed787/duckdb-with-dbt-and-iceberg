{{
    config(
        materialized='iceberg_incremental',
        incremental_strategy='delete+insert',
        unique_key='_source_file',
        partition_by=['month(pickup_at)'],
    )
}}

-- One row per valid trip. Keys join to dim_date, dim_time_of_day, dim_location,
-- dim_vendor, dim_rate_code and dim_payment_type.

{%- set source_files = pending_source_files(ref('silver_yellow_trips')) %}

select
    trip_id,
    strftime(pickup_at, '%Y%m%d')::integer                          as pickup_date_key,
    strftime(dropoff_at, '%Y%m%d')::integer                         as dropoff_date_key,
    hour(pickup_at)::integer                                        as pickup_hour_of_day,
    pickup_location_id,
    dropoff_location_id,
    vendor_id,
    rate_code_id,
    payment_type_id,
    pickup_at,
    dropoff_at,
    passenger_count,
    trip_distance_miles,
    trip_duration_minutes,
    round(trip_distance_miles / (trip_duration_minutes::double / 60), 2) as avg_speed_mph,
    fare_amount,
    extra_amount,
    mta_tax_amount,
    tip_amount,
    tolls_amount,
    improvement_surcharge_amount,
    congestion_surcharge_amount,
    airport_fee_amount,
    total_amount,
    -- tips are only recorded for card payments
    case
        when payment_type_id = 1 then round(tip_amount::double / fare_amount::double * 100, 2)
    end                                                             as tip_percentage,
    _source_file,
    _batch_id,
    now()                                                           as _processed_at
from {{ ref('silver_yellow_trips') }}
where {{ source_file_filter(source_files) }}
