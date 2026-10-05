{{ config(materialized='iceberg_table') }}

-- Payment types from the TLC data dictionary, plus any code that shows up in trips but is not listed.
with known as (
    select payment_type_id::integer as payment_type_id, payment_type_name::varchar as payment_type_name
    from {{ source('reference', 'payment_types') }}
),

observed as (
    select distinct payment_type_id
    from {{ ref('silver_yellow_trips') }}
    where payment_type_id is not null
)

select payment_type_id, payment_type_name
from known

union all

select observed.payment_type_id, concat('Unknown payment type ', observed.payment_type_id)
from observed
anti join known using (payment_type_id)
