{{
    config(
        materialized='iceberg_incremental',
        incremental_strategy='delete+insert',
        unique_key='_source_file',
        partition_by=['_source_period'],
    )
}}

-- Quarantine: bronze rows that failed a silver data-quality rule, with the reason.
-- A source file with zero rejected rows leaves no trace here, so it is re-checked on the
-- next run; that only re-reads the file and never changes the result.

{%- set source_files = pending_source_files(ref('bronze_yellow_tripdata')) %}

with classified as (
    {{ classify_yellow_trips(ref('bronze_yellow_tripdata'), source_files) }}
)

select
    _reject_reason,
    * exclude (_reject_reason),
    now() as _processed_at
from classified
where _reject_reason is not null
