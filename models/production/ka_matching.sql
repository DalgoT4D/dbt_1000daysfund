{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["ka"]
) }}

with current_rows as (
    select
        src.*,
        {{ profile_person_key('name_key', 'email', 'whatsapp_key', 'district_key') }} as profile_join_key
    from (
        select
            year,
            quarter,
            date,
            nullif(trim(name), '') as name,
            lower(nullif(trim(email), '')) as email,
            cast(null as integer) as age,
            cast(null as varchar) as education,
            program,
            nullif(trim(role), '') as role,
            nullif(trim(village), '') as village,
            is_certified,
            nullif(trim(district), '') as district,
            modul,
            nullif(trim(whatsapp), '') as whatsapp,
            nullif(trim(puskesmas), '') as puskesmas,
            true as is_latest,
            cast(score as integer) as score,
            coalesce(nullif(trim(unified_name), ''), nullif(trim(name), '')) as canonical_name,
            {{ profile_name_key('coalesce(unified_name, name)') }} as name_key,
            {{ profile_name_key('district') }} as district_key,
            {{ profile_whatsapp_key('whatsapp') }} as whatsapp_key
        from {{ ref('ka_all_clean') }}
        where date >= date '2026-04-01'
    ) src
),

profile_lookup_base as (
    select
        person_id,
        name as profile_name,
        lower(nullif(trim(email), '')) as email,
        nullif(trim(whatsapp), '') as whatsapp,
        nullif(trim(district), '') as district,
        {{ profile_name_key('name') }} as name_key,
        {{ profile_name_key('district') }} as district_key,
        {{ profile_whatsapp_key('whatsapp') }} as whatsapp_key
    from {{ ref('profile_fct') }}
),

profile_lookup as (
    select
        *,
        {{ profile_person_key('name_key', 'email', 'whatsapp_key', 'district_key') }} as profile_join_key
    from profile_lookup_base
),

matched_profiles as (
    select
        year,
        quarter,
        date,
        cr.name,
        cr.email,
        cr.age,
        cr.education,
        cr.program,
        cr.role,
        cr.village,
        cr.is_certified,
        cr.district,
        cr.modul,
        cr.whatsapp,
        cr.puskesmas,
        cr.is_latest,
        cr.score,
        cr.canonical_name,
        cr.name_key,
        cr.whatsapp_key,
        pl.person_id,
        pl.profile_name
    from current_rows cr
    left join profile_lookup pl
        on cr.profile_join_key = pl.profile_join_key
)

select
    year,
    quarter,
    date,
    name,
    email,
    age,
    education,
    program,
    role,
    village,
    is_certified,
    district,
    modul,
    whatsapp,
    puskesmas,
    is_latest,
    score,
    coalesce(person_id, concat(coalesce(email, whatsapp_key, name_key, 'unknown_person'), '_c0')) as person_id,
    coalesce(profile_name, canonical_name, name) as canonical_name
from matched_profiles
