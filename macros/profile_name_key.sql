{% macro profile_name_key(column_name) %}

    nullif(
        trim(
            regexp_replace(
                regexp_replace(
                    {{ normalize_unicode(column_name) }},
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                '[[:space:]]+',
                ' ',
                'g'
            )
        ),
        ''
    )

{% endmacro %}
