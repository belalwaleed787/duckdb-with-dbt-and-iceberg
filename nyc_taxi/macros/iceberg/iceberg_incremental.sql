{#-
  Incremental Iceberg table. Use is_incremental() in the model to select only new rows.

  First run or --full-refresh: create the table with CTAS (any old files are removed first);
  an existing table is replaced only after the new rows were computed successfully.
  Later runs:
    append         INSERT the model's rows.
    delete+insert  stage the model's rows in the local DuckDB file, then in one transaction
                   DELETE the rows whose unique_key appears in the new data and INSERT them.
                   Re-loading a key (e.g. a re-delivered source file) replaces its rows.

  The column list must stay the same between incremental runs; if it changes the run
  fails and asks for --full-refresh (DuckDB-Iceberg schema evolution is not used).

  Configs: location_root | location, partition_by, table_properties,
           incremental_strategy ('append' | 'delete+insert'), unique_key (one column)
-#}
{% materialization iceberg_incremental, adapter='duckdb' %}

    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set existing_relation = load_cached_relation(this) -%}
    {%- set location = iceberg_location(target_relation) -%}
    {%- set strategy = config.get('incremental_strategy') or 'append' -%}
    {%- set unique_key = config.get('unique_key') -%}

    {%- if strategy not in ['append', 'delete+insert'] -%}
        {{ exceptions.raise_compiler_error("iceberg_incremental supports incremental_strategy 'append' or 'delete+insert', got '" ~ strategy ~ "'") }}
    {%- endif -%}
    {%- if strategy == 'delete+insert' and unique_key is not string -%}
        {{ exceptions.raise_compiler_error("incremental_strategy 'delete+insert' needs unique_key set to one column name") }}
    {%- endif -%}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    {%- set rebuild_from = none -%}
    {%- set staging_relation = none -%}
    {%- if existing_relation is none or should_full_refresh() -%}
        {%- set rebuild_from = iceberg_prepare_rebuild(target_relation, existing_relation, location, compiled_code) -%}
        {%- call statement('main') -%}
            {{ iceberg_create_table_as(target_relation, location, rebuild_from.sql) }}
        {%- endcall -%}

    {%- else -%}
        {%- set changes = iceberg_schema_changes(target_relation, compiled_code) -%}
        {%- if changes | length > 0 -%}
            {{ exceptions.raise_compiler_error(
                "Columns of " ~ target_relation ~ " changed (" ~ changes | join(", ") ~ "). "
                ~ "Rebuild it with: dbt build --full-refresh --select " ~ model.name) }}
        {%- endif -%}

        {%- if strategy == 'append' -%}
            {%- call statement('main') -%}
                insert into {{ target_relation }} by name
                select * from (
                    {{ compiled_code }}
                ) as _model
            {%- endcall -%}

        {%- else -%}
            {%- set staging_relation = iceberg_staging_relation(target_relation) -%}
            {{ iceberg_stage(staging_relation, compiled_code) }}
            {%- set new_rows = run_query("select count(*) from " ~ staging_relation).columns[0].values()[0] -%}
            {{ log(model.name ~ ": " ~ new_rows ~ " new rows", info=True) }}

            {%- call statement('main') -%}
                {%- if new_rows > 0 %}
                delete from {{ target_relation }}
                where {{ unique_key }} in (select distinct {{ unique_key }} from {{ staging_relation }});
                insert into {{ target_relation }} by name
                select * from {{ staging_relation }}
                {%- else %}
                select 'nothing new' as status
                {%- endif %}
            {%- endcall -%}
        {%- endif -%}
    {%- endif -%}

    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ adapter.commit() }}
    {{ iceberg_drop_stage(staging_relation) }}
    {%- if rebuild_from is not none -%}
        {{ iceberg_drop_stage(rebuild_from.staging) }}
    {%- endif -%}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
