{{
    config(
        materialized='iceberg_incremental',
        incremental_strategy='delete+insert',
        unique_key='_source_file',
        partition_by=['month(pickup_at)'],
    )
}}

-- Cleaned, typed and de-duplicated trips. Rows that fail a rule are in silver_yellow_trips_rejected.

{%- set source_files = pending_source_files(ref('bronze_yellow_tripdata')) %}

with classified as (
    {{ classify_yellow_trips(ref('bronze_yellow_tripdata'), source_files) }}
)

select
    trip_id,
    vendor_id,
    pickup_at,
    dropoff_at,
    pickup_at::date                                         as pickup_date,
    round(trip_duration_seconds / 60.0, 2)::decimal(10, 2)  as trip_duration_minutes,
    passenger_count,
    trip_distance_miles,
    rate_code_id,
    is_store_and_forward,
    pickup_location_id,
    dropoff_location_id,
    payment_type_id,
    fare_amount,
    extra_amount,
    mta_tax_amount,
    tip_amount,
    tolls_amount,
    improvement_surcharge_amount,
    congestion_surcharge_amount,
    airport_fee_amount,
    total_amount,
    _source_file,
    _source_period,
    _batch_id,
    now()                                                   as _processed_at
from classified
where _reject_reason is null
