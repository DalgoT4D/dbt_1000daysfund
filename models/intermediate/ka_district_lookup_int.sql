{% set district_source_tables = ['ka_modul_01', 'ka_modul_02', 'ka_modul_03'] %}

{% set existing_typos_ctes %}
existing_typos_raw as (
    select lower(trim(typo)) as typo_key, trim(typo) as typo, nullif(trim(district), '') as district
    from reference.district_typos
    where nullif(trim(typo), '') is not null
),
existing_mappings as (
    select distinct on (typo_key) typo_key, typo, district
    from existing_typos_raw
    where district is not null
    order by typo_key, length(typo), typo
)
{% endset %}

{% set district_suggestions_query %}
with raw_districts as (
    {%- for table_name in district_source_tables %}
    select "Kabupaten_Kota" as district_value from raw_sheets."{{ table_name }}"{% if not loop.last %} union all{% endif %}
    {%- endfor %}
),

source_districts_base as (
    select
        {{ normalize_unicode('district_value') }} as district_raw,
        {{ ka_district_key('district_value') }} as district_match_key,
        {{ ka_district_key('district_value', strip_prefix=True) }} as district_match_key_stripped_raw
    from raw_districts
),

source_districts as (
    select
        district_raw,
        district_match_key,
        coalesce(nullif(district_match_key_stripped_raw, ''), district_match_key) as district_match_key_stripped,
        {{ ka_district_type_hint('district_raw') }} as district_type_hint,
        count(*) as observation_count
    from source_districts_base
    where coalesce(district_raw, '') <> ''
    group by 1, 2, 3, 4
),

{{ existing_typos_ctes }},

district_catalog_base as (
    select distinct
        trim(label) as district,
        {{ ka_district_key('label') }} as district_match_key,
        {{ ka_district_key('label', strip_prefix=True) }} as district_match_key_stripped_raw
    from reference."Kabupatenkota"
    where nullif(trim(label), '') is not null
),

district_catalog as (
    select
        district,
        district_match_key,
        coalesce(nullif(district_match_key_stripped_raw, ''), district_match_key) as district_match_key_stripped,
        {{ ka_district_type_hint('district_match_key') }} as district_type_hint
    from district_catalog_base
),

unmatched_districts as (
    select sd.*
    from source_districts sd
    left join existing_mappings em on sd.district_raw = em.typo_key
    where em.typo_key is null
        and sd.district_match_key not in ('-', 'kabupaten', 'kota', 'kotamadya', 'provinsi', 'propinsi')
        and length(sd.district_match_key) >= 4
),

candidate_matches as (
    select
        lower(trim(u.district_raw)) as typo_key,
        u.district_raw as typo,
        u.observation_count,
        c.district,
        case when u.district_match_key = c.district_match_key then 1 else 0 end as full_key_exact,
        case when u.district_match_key = c.district_match_key_stripped or u.district_match_key_stripped = c.district_match_key or u.district_match_key_stripped = c.district_match_key_stripped then 1 else 0 end as stripped_key_exact,
        case when u.district_type_hint is not null and u.district_type_hint = c.district_type_hint then 1 else 0 end as type_hint_match,
        case when u.district_match_key like '%' || c.district_match_key || '%' or u.district_match_key like '%' || c.district_match_key_stripped || '%' or u.district_match_key_stripped like '%' || c.district_match_key || '%' or u.district_match_key_stripped like '%' || c.district_match_key_stripped || '%' then 1 else 0 end as contains_match,
        greatest(
            similarity(u.district_match_key, c.district_match_key),
            similarity(u.district_match_key, c.district_match_key_stripped),
            similarity(u.district_match_key_stripped, c.district_match_key),
            similarity(u.district_match_key_stripped, c.district_match_key_stripped)
        ) as similarity_score
    from unmatched_districts u
    join district_catalog c
        on (
            u.district_match_key = c.district_match_key
            or u.district_match_key = c.district_match_key_stripped
            or u.district_match_key_stripped = c.district_match_key
            or u.district_match_key_stripped = c.district_match_key_stripped
            or u.district_match_key like '%' || c.district_match_key || '%'
            or u.district_match_key like '%' || c.district_match_key_stripped || '%'
            or u.district_match_key_stripped like '%' || c.district_match_key || '%'
            or u.district_match_key_stripped like '%' || c.district_match_key_stripped || '%'
            or similarity(u.district_match_key, c.district_match_key) >= 0.72
            or similarity(u.district_match_key, c.district_match_key_stripped) >= 0.72
            or similarity(u.district_match_key_stripped, c.district_match_key) >= 0.72
            or similarity(u.district_match_key_stripped, c.district_match_key_stripped) >= 0.72
        )
),

ranked_candidates as (
    select
        *,
        row_number() over district_match_rank as match_rank,
        lead(full_key_exact) over district_match_rank as next_full_key_exact,
        lead(type_hint_match) over district_match_rank as next_type_hint_match,
        lead(stripped_key_exact) over district_match_rank as next_stripped_key_exact,
        lead(contains_match) over district_match_rank as next_contains_match,
        lead(similarity_score) over district_match_rank as next_similarity_score
    from candidate_matches
    window district_match_rank as (
        partition by typo_key
        order by full_key_exact desc, type_hint_match desc, stripped_key_exact desc, contains_match desc, similarity_score desc, length(district) asc, district
    )
),

approved_matches as (
    select
        typo_key,
        typo,
        district,
        observation_count,
        case
            when full_key_exact = 1 then 'full_key_exact'
            when type_hint_match = 1 and stripped_key_exact = 1 then 'type_stripped_exact'
            when stripped_key_exact = 1 then 'stripped_exact'
            when contains_match = 1 then 'contains'
            else 'similarity'
        end as match_method,
        round(similarity_score::numeric, 4) as similarity_score
    from ranked_candidates
    where match_rank = 1
        and (
            full_key_exact = 1
            or (type_hint_match = 1 and stripped_key_exact = 1 and coalesce(next_full_key_exact, 0) = 0)
            or (stripped_key_exact = 1 and coalesce(next_full_key_exact, 0) = 0 and coalesce(next_type_hint_match, 0) = 0 and coalesce(next_similarity_score, 0) <= similarity_score - 0.08)
            or (contains_match = 1 and similarity_score >= 0.82 and coalesce(next_full_key_exact, 0) = 0 and coalesce(next_similarity_score, 0) <= similarity_score - 0.08)
            or (similarity_score >= 0.88 and coalesce(next_full_key_exact, 0) = 0 and coalesce(next_contains_match, 0) = 0 and coalesce(next_similarity_score, 0) <= similarity_score - 0.08)
        )
)

select typo_key, typo, district, observation_count, match_method, similarity_score
from approved_matches
{% endset %}

{% set district_typos_update_sql %}
update reference.district_typos as dt
set district = suggestions.district
from (
{{ district_suggestions_query }}
) as suggestions
where lower(trim(dt.typo)) = suggestions.typo_key
    and coalesce(trim(dt.district), '') = ''
{% endset %}

{% set district_typos_insert_sql %}
insert into reference.district_typos (typo, district)
select suggestions.typo, suggestions.district
from (
{{ district_suggestions_query }}
) as suggestions
where not exists (
    select 1
    from reference.district_typos dt
    where lower(trim(dt.typo)) = suggestions.typo_key
)
{% endset %}

{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    post_hook=[district_typos_update_sql, district_typos_insert_sql],
    quoting={'identifier': true},
    tags=["ka", "intermediate"]
) }}

with
{{ existing_typos_ctes }},

suggested_matches as (
    {{ district_suggestions_query }}
),

final_lookup as (
    select typo_key, typo, district, 'reference' as mapping_source, 'reference' as match_method, null::bigint as observation_count, null::numeric as similarity_score
    from existing_mappings

    union all

    select sm.typo_key, sm.typo, sm.district, 'suggested' as mapping_source, sm.match_method, sm.observation_count, sm.similarity_score
    from suggested_matches sm
    where not exists (
        select 1
        from existing_mappings em
        where em.typo_key = sm.typo_key
    )
)

select typo_key, typo, district, mapping_source, match_method, observation_count, similarity_score
from final_lookup
