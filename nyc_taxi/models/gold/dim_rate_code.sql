{{ config(materialized='iceberg_table') }}

-- Rate codes from the TLC data dictionary, plus any code that shows up in trips but is not listed.
with known as (
    select rate_code_id::integer as rate_code_id, rate_code_name::varchar as rate_code_name
    from {{ source('reference', 'rate_codes') }}
),

observed as (
    select distinct rate_code_id
    from {{ ref('silver_yellow_trips') }}
    where rate_code_id is not null
)

select rate_code_id, rate_code_name
from known

union all

select observed.rate_code_id, concat('Unknown rate code ', observed.rate_code_id)
from observed
anti join known using (rate_code_id)
