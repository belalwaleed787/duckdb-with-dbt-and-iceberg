{#-
  Cleans bronze yellow taxi rows for the silver layer.

  Renames and types the raw columns, finds exact duplicates and tags each row with the
  first data-quality rule it breaks (_reject_reason is NULL for a valid row).
  silver_yellow_trips keeps the valid rows, silver_yellow_trips_rejected the others.
  Thresholds are dbt vars (see dbt_project.yml).
-#}
{% macro classify_yellow_trips(bronze_relation, source_files) %}
with bronze as (
    select *
    from {{ bronze_relation }}
    where {{ source_file_filter(source_files) }}
),

typed as (
    select
        -- yyyymm * 10^8 + row number inside the landing file: stable, unique and traceable
        (year(_source_period) * 100 + month(_source_period))::bigint * 100000000
            + _source_row_number                                as trip_id,
        vendorid::integer                                       as vendor_id,
        tpep_pickup_datetime                                    as pickup_at,
        tpep_dropoff_datetime                                   as dropoff_at,
        date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_seconds,
        -- 0 or more than 6 passengers is not a real count
        case when passenger_count between 1 and 6 then passenger_count::integer end as passenger_count,
        trip_distance                                           as trip_distance_miles,
        coalesce(ratecodeid::integer, 99)                       as rate_code_id,
        case store_and_fwd_flag when 'Y' then true when 'N' then false end as is_store_and_forward,
        pulocationid::integer                                   as pickup_location_id,
        dolocationid::integer                                   as dropoff_location_id,
        payment_type::integer                                   as payment_type_id,
        fare_amount::decimal(10, 2)                             as fare_amount,
        coalesce(extra, 0)::decimal(10, 2)                      as extra_amount,
        coalesce(mta_tax, 0)::decimal(10, 2)                    as mta_tax_amount,
        coalesce(tip_amount, 0)::decimal(10, 2)                 as tip_amount,
        coalesce(tolls_amount, 0)::decimal(10, 2)               as tolls_amount,
        coalesce(improvement_surcharge, 0)::decimal(10, 2)      as improvement_surcharge_amount,
        coalesce(congestion_surcharge, 0)::decimal(10, 2)       as congestion_surcharge_amount,
        coalesce(airport_fee, 0)::decimal(10, 2)                as airport_fee_amount,
        total_amount::decimal(10, 2)                            as total_amount,
        _source_file,
        _source_row_number,
        _source_period,
        _batch_id
    from bronze
),

ranked as (
    select
        *,
        row_number() over (
            partition by
                _source_file, vendor_id, pickup_at, dropoff_at, pickup_location_id,
                dropoff_location_id, trip_distance_miles, fare_amount, total_amount, payment_type_id
            order by _source_row_number
        ) as duplicate_rank
    from typed
)

select
    * exclude (duplicate_rank),
    case
        when duplicate_rank > 1
            then 'duplicate'
        when _source_period is null
            then 'unknown_source_period'
        when pickup_at is null or dropoff_at is null
            then 'missing_timestamp'
        when date_trunc('month', pickup_at) <> _source_period
            then 'pickup_outside_file_month'
        when trip_duration_seconds < {{ var('min_trip_duration_minutes') }} * 60
          or trip_duration_seconds > {{ var('max_trip_duration_hours') }} * 3600
            then 'invalid_duration'
        when trip_distance_miles is null
          or trip_distance_miles <= 0
          or trip_distance_miles > {{ var('max_trip_distance_miles') }}
            then 'invalid_distance'
        when trip_distance_miles / (trip_duration_seconds / 3600.0) > {{ var('max_avg_speed_mph') }}
            then 'implausible_speed'
        when fare_amount is null or fare_amount <= 0
          or total_amount is null or total_amount <= 0
          or fare_amount > {{ var('max_fare_amount') }}
            then 'invalid_amount'
        when pickup_location_id not between 1 and 265
          or dropoff_location_id not between 1 and 265
            then 'unknown_location'
    end as _reject_reason
from ranked
{% endmacro %}
