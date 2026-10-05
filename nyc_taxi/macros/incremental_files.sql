{#-
  File-level incremental loading.

  Bronze remembers which landing file every row came from (_source_file) and that file's
  S3 last-modified time. Every layer also keeps the _batch_id (dbt invocation) that loaded
  the file into bronze, so a re-delivered file flows through silver and gold as well.
-#}

{#- Landing files that are new, or whose S3 last-modified time changed since they were loaded. -#}
{% macro pending_landing_files(landing_glob) %}
    {%- if not execute -%}
        {{ return([]) }}
    {%- endif -%}
    {%- set query -%}
        select landing.filename
        from read_blob('{{ landing_glob }}') as landing
        {%- if is_incremental() %}
        anti join (
            select distinct _source_file, _source_modified_at from {{ this }}
        ) as loaded
            on landing.filename = loaded._source_file
           and landing.last_modified = loaded._source_modified_at
        {%- endif %}
        order by 1
    {%- endset -%}
    {{ return(run_query(query).columns[0].values() | list) }}
{% endmacro %}


{#- Source files whose latest bronze batch has not reached this model yet; none means "all". -#}
{% macro pending_source_files(upstream) %}
    {%- if not execute or not is_incremental() -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set query -%}
        select upstream._source_file
        from (select distinct _source_file, _batch_id from {{ upstream }}) as upstream
        anti join (
            select distinct _source_file, _batch_id from {{ this }}
        ) as loaded
            on upstream._source_file = loaded._source_file
           and upstream._batch_id = loaded._batch_id
        order by 1
    {%- endset -%}
    {{ return(run_query(query).columns[0].values() | list) }}
{% endmacro %}


{#- SQL predicate for a pending_source_files() result. -#}
{% macro source_file_filter(files) -%}
    {%- if files is none -%}
        true
    {%- elif files | length == 0 -%}
        false
    {%- else -%}
        _source_file in (
            {%- for file in files %}
            '{{ file }}'{{ "," if not loop.last }}
            {%- endfor %}
        )
    {%- endif -%}
{%- endmacro %}
