{#-
  prod: use the configured schema (Glue database) as-is, e.g. nyc_bronze_duck.
  any other target: prefix it with the target schema, e.g. dev_nyc_bronze_duck.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
