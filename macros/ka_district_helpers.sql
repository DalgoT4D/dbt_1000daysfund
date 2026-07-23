{% macro ka_district_key(column_name, strip_prefix=False) -%}

    {% set district_prefix_regex = '^(kabupaten|kab[[:punct:]]?|kota administrasi|kotamadya|kota)[[:space:]]+' %}

    trim(
        regexp_replace(
            regexp_replace(
                {% if strip_prefix %}
                    regexp_replace({{ normalize_unicode(column_name) }}, '{{ district_prefix_regex }}', '')
                {% else %}
                    {{ normalize_unicode(column_name) }}
                {% endif %},
                '[^a-z0-9]+',
                ' ',
                'g'
            ),
            '[[:space:]]+',
            ' ',
            'g'
        )
    )

{%- endmacro %}

{% macro ka_district_type_hint(column_name) -%}

    {% set kota_regex = '(^|[[:space:]])kota([[:space:]]|$)|kotamadya|kota administrasi' %}
    {% set kabupaten_regex = '(^|[[:space:]])kabupaten([[:space:]]|$)|(^|[[:space:]])kab[[:punct:]]?([[:space:]]|$)' %}

    case
        when {{ column_name }} ~ '{{ kota_regex }}' then 'kota'
        when {{ column_name }} ~ '{{ kabupaten_regex }}' then 'kabupaten'
        else null
    end

{%- endmacro %}
