{% macro profile_person_key(name_key, email, whatsapp_key, district_key) %}

    coalesce(
        case
            when {{ name_key }} is not null and {{ email }} is not null
                then concat({{ name_key }}, '||', {{ email }})
            else null
        end,
        case
            when {{ name_key }} is not null and {{ whatsapp_key }} is not null
                then concat({{ name_key }}, '||', {{ whatsapp_key }})
            else null
        end,
        case
            when {{ name_key }} is not null and {{ district_key }} is not null
                then concat({{ name_key }}, '||', {{ district_key }})
            else null
        end,
        {{ email }},
        {{ whatsapp_key }},
        {{ name_key }}
    )

{% endmacro %}
