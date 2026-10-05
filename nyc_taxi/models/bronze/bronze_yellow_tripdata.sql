{{
    config(
        materialized='iceberg_incremental',
        incremental_strategy='delete+insert',
        unique_key='_source_file',
        partition_by=['_source_period'],
    )
}}

-- Raw landing data as an Iceberg table: source values untouched, column names lower-cased,
-- plus lineage columns. Only new or re-delivered landing files are read on each run.
-- depends_on: {{ source('landing', 'yellow_tripdata') }}

{%- set landing_glob = var('landing_glob') %}
{%- set files = pending_landing_files(landing_glob) %}

{%- if execute and files | length == 0 and not is_incremental() %}
    {{ exceptions.raise_compiler_error("No landing files match " ~ landing_glob) }}
{%- endif %}

{%- if files | length == 0 %}

-- Landing has nothing new: produce no rows.
select * from {{ this }} where false

{%- else %}

with landing_files as (
    select
        filename as _source_file,
        last_modified as _source_modified_at
    from read_blob('{{ landing_glob }}')
),

expected_columns as (
    -- Typed, empty frame. UNION ALL BY NAME matches names case-insensitively
    -- ('Airport_fee' vs 'airport_fee') and fills columns a file does not have with NULL.
    select
        null::bigint    as vendorid,
        null::timestamp as tpep_pickup_datetime,
        null::timestamp as tpep_dropoff_datetime,
        null::double    as passenger_count,
        null::double    as trip_distance,
        null::double    as ratecodeid,
        null::varchar   as store_and_fwd_flag,
        null::bigint    as pulocationid,
        null::bigint    as dolocationid,
        null::bigint    as payment_type,
        null::double    as fare_amount,
        null::double    as extra,
        null::double    as mta_tax,
        null::double    as tip_amount,
        null::double    as tolls_amount,
        null::double    as improvement_surcharge,
        null::double    as total_amount,
        null::double    as congestion_surcharge,
        null::double    as airport_fee,
        null::varchar   as filename,
        null::bigint    as file_row_number
    limit 0
),

raw as (
    select * from expected_columns
    union all by name
    select *
    from read_parquet(
        [
            {%- for file in files %}
            '{{ file }}'{{ "," if not loop.last }}
            {%- endfor %}
        ],
        union_by_name = true,
        filename = true,
        file_row_number = true
    )
)

select
    raw.vendorid::bigint                    as vendorid,
    raw.tpep_pickup_datetime::timestamp     as tpep_pickup_datetime,
    raw.tpep_dropoff_datetime::timestamp    as tpep_dropoff_datetime,
    raw.passenger_count::double             as passenger_count,
    raw.trip_distance::double               as trip_distance,
    raw.ratecodeid::double                  as ratecodeid,
    raw.store_and_fwd_flag::varchar         as store_and_fwd_flag,
    raw.pulocationid::bigint                as pulocationid,
    raw.dolocationid::bigint                as dolocationid,
    raw.payment_type::bigint                as payment_type,
    raw.fare_amount::double                 as fare_amount,
    raw.extra::double                       as extra,
    raw.mta_tax::double                     as mta_tax,
    raw.tip_amount::double                  as tip_amount,
    raw.tolls_amount::double                as tolls_amount,
    raw.improvement_surcharge::double       as improvement_surcharge,
    raw.total_amount::double                as total_amount,
    raw.congestion_surcharge::double        as congestion_surcharge,
    raw.airport_fee::double                 as airport_fee,

    raw.filename                            as _source_file,
    raw.file_row_number                     as _source_row_number,
    landing_files._source_modified_at,
    -- yellow_tripdata_2023_01.parquet -> 2023-01-01 (NULL if the name has no year/month)
    make_date(
        try_cast(regexp_extract(raw.filename, '(\d{4})[_-](\d{2})\.parquet$', 1) as integer),
        try_cast(regexp_extract(raw.filename, '(\d{4})[_-](\d{2})\.parquet$', 2) as integer),
        1
    )                                       as _source_period,
    '{{ invocation_id }}'                   as _batch_id,
    now()                                   as _ingested_at
from raw
inner join landing_files
    on raw.filename = landing_files._source_file

{%- endif %}
