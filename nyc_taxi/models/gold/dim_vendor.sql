{{ config(materialized='iceberg_table') }}

-- Vendors from the TLC data dictionary, plus any code that shows up in trips but is not listed.
with known as (
    select vendor_id::integer as vendor_id, vendor_name::varchar as vendor_name
    from {{ source('reference', 'vendors') }}
),

observed as (
    select distinct vendor_id
    from {{ ref('silver_yellow_trips') }}
    where vendor_id is not null
)

select vendor_id, vendor_name
from known

union all

select observed.vendor_id, concat('Unknown vendor ', observed.vendor_id)
from observed
anti join known using (vendor_id)
