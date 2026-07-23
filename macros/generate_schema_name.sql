{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema | trim -%}
    {%- set folder_schema = node.fqn[1:-1] | join('_') | trim -%}

    {%- if custom_schema_name is not none and custom_schema_name | trim != '' -%}
        {%- set resolved_schema = custom_schema_name | trim -%}
    {%- elif folder_schema != '' -%}
        {%- set resolved_schema = folder_schema -%}
    {%- else -%}
        {%- set resolved_schema = default_schema -%}
    {%- endif -%}

    {%- if default_schema | lower == 'dev' and resolved_schema != default_schema -%}
        {{ default_schema }}_{{ resolved_schema }}
    {%- else -%}
        {{ resolved_schema }}
    {%- endif -%}

{%- endmacro %}
