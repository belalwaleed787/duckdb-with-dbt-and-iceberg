{#- Same as dbt's built-in is_incremental(), but also true for the iceberg_incremental materialization. -#}
{% macro is_incremental() %}
    {% if not execute %}
        {{ return(false) }}
    {% endif %}
    {% if model.config.materialized not in ['incremental', 'iceberg_incremental'] %}
        {{ return(false) }}
    {% endif %}
    {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
    {{ return(relation is not none and relation.type == 'table' and not should_full_refresh()) }}
{% endmacro %}
