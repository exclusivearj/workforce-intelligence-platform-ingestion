{#
    Use the schema configured on the model (+schema) as the literal target schema,
    rather than dbt's default of prefixing it with the profile target schema.
    This lets staging land in `staging` and marts in `analytics` directly.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
