{#-
  Shared helpers for the iceberg_table and iceberg_incremental materializations.

  DuckDB writes Iceberg through the Glue catalog attached as "lakehouse". Glue needs an
  explicit table location, has no RENAME and refuses purge on DROP, so these
  materializations never rename a table:
    - create:    CREATE TABLE ... PARTITIONED BY (...) WITH ('location' = ...) AS SELECT
    - overwrite: DELETE + INSERT in one transaction (a single atomic Iceberg commit)
    - rebuild:   run the model into a local staging table, then DROP (catalog entry),
                 empty the S3 folder and CREATE from the staging table
  Statements that must not share a transaction run with auto_begin=False.
-#}

{% macro iceberg_location(relation) -%}
    {%- set location = config.get('location') -%}
    {%- if not location -%}
        {%- set root = config.get('location_root') -%}
        {%- if not root -%}
            {{ exceptions.raise_compiler_error("Model " ~ relation ~ " needs a 'location' or 'location_root' config") }}
        {%- endif -%}
        {%- set location = root.rstrip('/') ~ '/' ~ relation.identifier -%}
    {%- endif -%}
    {{ return(location.rstrip('/')) }}
{%- endmacro %}


{% macro iceberg_create_table_as(relation, location, sql) -%}
    {%- set partition_by = config.get('partition_by') -%}
    {%- if partition_by is string -%}
        {%- set partition_by = [partition_by] -%}
    {%- endif -%}
    {%- set properties = {'location': location, 'format-version': '2'} -%}
    {%- do properties.update(config.get('table_properties') or {}) -%}

    create table {{ relation }}
    {%- if partition_by %}
    partitioned by ({{ partition_by | join(', ') }})
    {%- endif %}
    with (
    {%- for key, value in properties.items() %}
        '{{ key }}' = '{{ value }}'{{ "," if not loop.last }}
    {%- endfor %}
    )
    as
    select * from (
        {{ sql }}
    ) as _model
{%- endmacro %}


{% macro iceberg_drop_table(relation) %}
    {#- Runs outside a transaction: DuckDB cannot drop and re-create the same table in one transaction. -#}
    {%- call statement('iceberg_drop_table', auto_begin=False) -%}
        drop table if exists {{ relation }}
    {%- endcall -%}
{% endmacro %}


{% macro iceberg_clear_location(location) %}
    {#- Deletes every object under the table folder (see plugins/iceberg_helpers.py). -#}
    {%- call statement('iceberg_clear_location', fetch_result=True, auto_begin=False) -%}
        select iceberg_purge_location('{{ location }}') as deleted_objects
    {%- endcall -%}
    {%- set deleted = load_result('iceberg_clear_location')['data'][0][0] -%}
    {%- if deleted > 0 -%}
        {{ log("Removed " ~ deleted ~ " old objects under " ~ location ~ "/", info=True) }}
    {%- endif -%}
{% endmacro %}


{#-
  Staging = a regular table in the container's local DuckDB file (compressed on disk, so a
  batch of tens of millions of rows does not have to fit in memory). It is written in its own
  transaction because one DuckDB transaction can only write to one database.
-#}
{% macro iceberg_staging_relation(target_relation) %}
    {{ return(api.Relation.create(
        database=target.database, schema='main', identifier=target_relation.identifier ~ '__dbt_stage')) }}
{% endmacro %}


{% macro iceberg_stage(staging_relation, sql) %}
    {%- call statement('iceberg_stage', auto_begin=False) -%}
        drop table if exists {{ staging_relation }};
        create table {{ staging_relation }} as
        select * from (
            {{ sql }}
        ) as _model
    {%- endcall -%}
{% endmacro %}


{% macro iceberg_drop_stage(staging_relation) %}
    {%- if staging_relation is not none -%}
        {%- call statement('iceberg_drop_stage', auto_begin=False) -%}
            drop table if exists {{ staging_relation }}
        {%- endcall -%}
    {%- endif -%}
{% endmacro %}


{#-
  Prepares a (re)build with CTAS and returns {'sql': query to build from, 'staging': relation or none}.
  An existing table is dropped only after the model's query has completed into the local
  staging table, so a failing model never leaves the table missing.
-#}
{% macro iceberg_prepare_rebuild(target_relation, existing_relation, location, sql) %}
    {%- if existing_relation is none -%}
        {{ iceberg_clear_location(location) }}
        {{ return({'sql': sql, 'staging': none}) }}
    {%- endif -%}
    {%- set staging_relation = iceberg_staging_relation(target_relation) -%}
    {{ iceberg_stage(staging_relation, sql) }}
    {{ iceberg_drop_table(target_relation) }}
    {{ iceberg_clear_location(location) }}
    {{ return({'sql': 'select * from ' ~ staging_relation, 'staging': staging_relation}) }}
{% endmacro %}


{% macro iceberg_columns(sql_or_relation) %}
    {%- set result = run_query("describe " ~ sql_or_relation) -%}
    {%- set columns = {} -%}
    {%- for row in result.rows -%}
        {%- do columns.update({(row['column_name'] | lower): row['column_type']}) -%}
    {%- endfor -%}
    {{ return(columns) }}
{% endmacro %}


{% macro iceberg_schema_changes(relation, sql) %}
    {#- Compares the model's output columns with the existing table; returns human-readable differences. -#}
    {%- set existing = iceberg_columns(relation) -%}
    {%- set incoming = iceberg_columns("select * from (" ~ sql ~ ") as _model") -%}
    {%- set changes = [] -%}
    {%- for name, data_type in incoming.items() -%}
        {%- if name not in existing -%}
            {%- do changes.append("added " ~ name ~ " " ~ data_type) -%}
        {%- elif existing[name] != data_type -%}
            {%- do changes.append(name ~ " changed " ~ existing[name] ~ " -> " ~ data_type) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- for name in existing -%}
        {%- if name not in incoming -%}
            {%- do changes.append("removed " ~ name) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(changes) }}
{% endmacro %}
