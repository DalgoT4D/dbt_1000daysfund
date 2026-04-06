{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set model_folders = node.fqn[1:-1] -%}

    {%- if 'elementary' in node.fqn -%}
        {{ target.schema }}_elementary
    {%- elif custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- elif model_folders | length > 0 -%}
        {{ model_folders[0] | trim }}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}

{%- endmacro %}
