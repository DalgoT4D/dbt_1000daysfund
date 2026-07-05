{% macro profile_whatsapp_key(column_name) %}

    nullif(
        regexp_replace(
            coalesce({{ column_name }}, ''),
            '[^0-9]+',
            '',
            'g'
        ),
        ''
    )

{% endmacro %}
