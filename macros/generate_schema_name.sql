{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {# Handle specific cases based on folder names or tags #}
        {% if 'elementary' in node.fqn %}
            {{ target.schema }}_elementary

        {% elif 'staging' in node.fqn and node.fqn.index('staging') + 1 < node.fqn | length %}
            {% set prefix = node.fqn[node.fqn.index('staging')] %}
            intermediate_{{ prefix | trim }}

        {% elif 'marts' in node.fqn and node.fqn.index('marts') + 1 < node.fqn | length %}
            {% set prefix = node.fqn[node.fqn.index('marts')] %}
            {{ target.schema }}_{{ prefix | trim }}

        {# Fallback to default schema if no specific case matches #}
        {% else %}
            {{ default_schema }}
        {% endif %}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}