{#-
  Iceberg table that is fully recomputed on every run.

  First run (or --full-refresh, or a changed column list): create the table with CTAS;
  an existing table is replaced only after the new rows were computed successfully.
  Later runs: DELETE + INSERT inside one transaction, so readers (Athena, DuckDB, ...)
  never see a missing or half-written table and Iceberg keeps the previous snapshot.

  Configs: location_root | location, partition_by, table_properties
-#}
{% materialization iceberg_table, adapter='duckdb' %}

    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set existing_relation = load_cached_relation(this) -%}
    {%- set location = iceberg_location(target_relation) -%}
    {%- set rebuild = existing_relation is none or should_full_refresh() -%}
    {%- set rebuild_from = none -%}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    {%- if not rebuild -%}
        {%- set changes = iceberg_schema_changes(target_relation, compiled_code) -%}
        {%- if changes | length > 0 -%}
            {{ log("Columns of " ~ target_relation ~ " changed (" ~ changes | join(", ") ~ "); rebuilding the table", info=True) }}
            {%- set rebuild = true -%}
        {%- endif -%}
    {%- endif -%}

    {%- if rebuild -%}
        {%- set rebuild_from = iceberg_prepare_rebuild(target_relation, existing_relation, location, compiled_code) -%}
        {%- call statement('main') -%}
            {{ iceberg_create_table_as(target_relation, location, rebuild_from.sql) }}
        {%- endcall -%}
    {%- else -%}
        {%- call statement('main') -%}
            delete from {{ target_relation }};
            insert into {{ target_relation }} by name
            select * from (
                {{ compiled_code }}
            ) as _model
        {%- endcall -%}
    {%- endif -%}

    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ adapter.commit() }}
    {%- if rebuild_from is not none -%}
        {{ iceberg_drop_stage(rebuild_from.staging) }}
    {%- endif -%}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
