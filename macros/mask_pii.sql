{% macro mask_pii(column_name, mask_type) %}
    {% if mask_type == 'email' %}
        REGEXP_REPLACE({{ column_name }}, '(^[a-zA-Z0-9._%+-]{2})[a-zA-Z0-9._%+-]+(@.*)', '\\1****\\2')
    {% elif mask_type == 'phone' %}
        REGEXP_REPLACE({{ column_name }}, '.*([0-9]{4})$', '**** **** \\1')
    {% elif mask_type == 'name' %}
        REGEXP_REPLACE({{ column_name }}, '^(\\w)(.*)$', '\\1****')
    {% else %}
        'MASKED'
    {% endif %}
{% endmacro %}