{% macro first_existing_column(relation, candidates, fallback='null') %}

    {% if execute %}
        {% set relation_columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list %}
        {% for candidate in candidates %}
            {% if candidate in relation_columns %}
                {{ return(adapter.quote(candidate)) }}
            {% endif %}
        {% endfor %}
    {% endif %}

    {{ return(fallback) }}

{% endmacro %}
