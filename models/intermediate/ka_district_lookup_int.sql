-- Model: district name lookup table for KA modules 1-3.
--
-- The problem: "district" in the source sheets is free text. People type
-- abbreviations, typos, and spelling variants instead of the official district
-- name. This model is the single place that resolves a typed value to one
-- canonical district name. Staging models (ka_modul_1/2/3_clean) join to this
-- table's `typo_key` to turn whatever was typed into a clean `district`.
--
-- How the mapping is built, step by step:
--   1. existing_mappings   - corrections a human has already approved, read
--                             from reference.district_typos.
--   2. source_districts    - every distinct district value actually typed
--                             across modules 1-3, normalized for comparison.
--   3. district_catalog    - the official district list (reference_district_clean),
--                             normalized the same way as the typed values.
--   4. unmatched_districts - typed values that don't already have an approved
--                             mapping, with obvious junk filtered out.
--   5. candidate_matches -> ranked_candidates -> approved_matches
--                           - fuzzy-match each unmatched value against the
--                             official catalog, rank the candidates by match
--                             strength, and keep only matches confident enough
--                             to auto-approve (see approved_matches below for
--                             the exact rule).
--   6. final_lookup        - approved reference mappings + newly approved
--                             suggestions = this model's output.
--
-- Why the post-hooks write back into reference.district_typos: once a
-- suggestion is approved it needs to become permanent, otherwise it would be
-- silently re-derived (or dropped) every run from whatever the source sheets
-- happen to contain that day. Writing it back locks the correction in place
-- regardless of future source-data changes.
--
-- The post-hooks read the 'suggested' rows straight out of this model's own
-- freshly-built table (intermediate.ka_district_lookup_int, written as a
-- literal - same as reference.district_typos below) rather than re-running
-- the matching query a second and third time.
{% set district_source_tables = ['ka_modul_01', 'ka_modul_02', 'ka_modul_03'] %}

{% set existing_typos_ctes %}
-- Human-approved typo -> district corrections already sitting in
-- reference.district_typos. A typo can have duplicate rows there; keep just
-- one (the shortest, cleanest-looking) per normalized typo.
existing_mappings as (
    select distinct on (lower(trim(typo)))
        lower(trim(typo)) as typo_key,
        trim(typo) as typo,
        nullif(trim(district), '') as district
    from reference.district_typos
    where nullif(trim(typo), '') is not null
      and nullif(trim(district), '') is not null
    order by
        lower(trim(typo)),
        length(trim(typo)),
        trim(typo)
)
{% endset %}

{% set district_suggestions_query %}
-- Pull every district value people have typed across modules 1-3, so we can
-- look for corrections among the ones that don't have an approved mapping yet.
with raw_districts as (
    {%- for table_name in district_source_tables %}
    select "Kabupaten_Kota" as district_value from raw_sheets."{{ table_name }}"{% if not loop.last %} union all{% endif %}
    {%- endfor %}
),

-- Build two comparison keys for each typed value: the full normalized text,
-- and the same text with a district-type prefix ("Kabupaten"/"Kota"/etc.)
-- stripped off, so "Kab. Badung" and "Badung" can still be compared.
source_districts_base as (
    select
        {{ normalize_unicode('district_value') }} as district_raw,
        {{ ka_district_key('district_value') }} as district_match_key,
        {{ ka_district_key('district_value', strip_prefix=True) }} as district_match_key_stripped_raw
    from raw_districts
),

-- Collapse to one row per distinct typed value. observation_count (how many
-- times it was typed) is carried through for visibility only - it does not
-- affect whether a match gets approved below.
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

-- Load the official district list and build the same two comparison keys for
-- it, so it can be matched against source_districts on equal footing.
-- reference_district_clean has already dropped the redundant "Kabupaten"
-- prefix, so district here is already the clean label we want to output.
district_catalog_base as (
    select distinct
        district,
        {{ ka_district_key('district') }} as district_match_key,
        {{ ka_district_key('district', strip_prefix=True) }} as district_match_key_stripped_raw
    from {{ ref('reference_district_clean') }}
),

-- Fall back to the full key when there's no separate stripped form (e.g. no
-- prefix to strip), and tag each official district as 'kota' or 'kabupaten'
-- so that type can be used as a matching signal too.
district_catalog as (
    select
        district,
        district_match_key,
        coalesce(nullif(district_match_key_stripped_raw, ''), district_match_key) as district_match_key_stripped,
        {{ ka_district_type_hint('district_match_key') }} as district_type_hint
    from district_catalog_base
),

-- Only attempt to auto-correct typed values that (a) don't already have an
-- approved mapping, and (b) aren't junk - bare placeholders like "-", or bare
-- type words like "kabupaten" with nothing else, or text too short to match
-- reliably.
unmatched_districts as (
    select sd.*
    from source_districts sd
    left join existing_mappings em on sd.district_raw = em.typo_key
    where em.typo_key is null
        and sd.district_match_key not in ('-', 'kabupaten', 'kota', 'kotamadya', 'provinsi', 'propinsi')
        and length(sd.district_match_key) >= 4
),

-- For every typed value that could plausibly be a given official district
-- (exact match, prefix-stripped match, substring match, or at least 72%
-- text-similar), compute the individual match signals used below to rank
-- candidates and decide whether a match is trustworthy enough to approve.
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

-- For each typed value, order its candidate districts from strongest to
-- weakest match. lead() also grabs the runner-up's scores on the same row,
-- which approved_matches needs below to check the winner isn't a near-tie.
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

-- Auto-approve the top-ranked candidate only when it's clearly the right
-- district, not a guess:
--   * an exact key match, always wins outright; or
--   * an exact match once type prefixes are stripped, as long as the type
--     (kota/kabupaten) also matches and there's no tied full-key competitor; or
--   * a stripped-key or substring/similarity match, as long as there's no
--     equally strong competing candidate and the runner-up trails by a
--     comfortable margin (>= 0.08 similarity)
-- Anything less certain than that is left unmapped rather than guessed at.
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

-- Post-hook 1: for typo rows that already exist in reference.district_typos
-- but have no district filled in yet, fill it in with whatever this run
-- approved. Reads from this model's own 'suggested' rows once it's built -
-- same table/schema this model always writes to, written out directly
-- (matching how reference.district_typos is a plain literal below too).
{% set district_typos_update_sql %}
update reference.district_typos as dt
set district = suggestions.district
from (
    select typo_key, typo, district
    from intermediate.ka_district_lookup_int
    where mapping_source = 'suggested'
) as suggestions
where lower(trim(dt.typo)) = suggestions.typo_key
    and coalesce(trim(dt.district), '') = ''
{% endset %}

-- Post-hook 2: for typo values that have never been seen before, insert them
-- as new approved rows, so next run treats them as an existing reference
-- mapping instead of re-deriving a suggestion.
{% set district_typos_insert_sql %}
insert into reference.district_typos (typo, district)
select suggestions.typo, suggestions.district
from (
    select typo_key, typo, district
    from intermediate.ka_district_lookup_int
    where mapping_source = 'suggested'
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

-- Run the fuzzy-matching logic above to get this run's high-confidence
-- suggestions for districts with no approved mapping yet.
suggested_matches as (
    {{ district_suggestions_query }}
),

-- The model's output: every already-approved reference mapping, plus any new
-- suggestion that isn't already covered by one (existing approved mappings
-- always win over a freshly suggested one for the same typo).
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
