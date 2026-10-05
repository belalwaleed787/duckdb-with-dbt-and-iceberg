{{ config(materialized='iceberg_table') }}

select
    location_id::integer                    as location_id,
    borough::varchar                        as borough,
    zone_name::varchar                      as zone_name,
    service_zone::varchar                   as service_zone,
    location_id::integer in (1, 132, 138)   as is_airport  -- Newark, JFK, LaGuardia
from {{ source('reference', 'taxi_zone_lookup') }}
