-- Model: Matches current KA participation records to canonical profiles.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["ka"]
) }}

-- Prepare latest current-module records for profile matching.
with current_rows as (
    select
        src.*,
        {{ profile_person_key('name_key', 'email', 'whatsapp_key', 'district_key') }} as profile_join_key
    from (
        select
            year,
            quarter,
            date,
            nullif(trim(nama), '') as nama,
            lower(nullif(trim(email), '')) as email,
            cast(null as integer) as age,
            cast(null as varchar) as education,
            program,
            nullif(trim(role), '') as role,
            nullif(trim(desa_kelurahan), '') as desa_kelurahan,
            is_certified,
            nullif(trim(kota_kabupaten), '') as kota_kabupaten,
            modul,
            nullif(trim(whatsapp), '') as whatsapp,
            nullif(trim(puskesmas), '') as puskesmas,
            is_latest,
            cast(score as integer) as score,
            coalesce(nullif(trim(unified_nama), ''), nullif(trim(nama), '')) as canonical_nama,
            {{ profile_name_key('coalesce(unified_nama, nama)') }} as name_key,
            {{ profile_name_key('kota_kabupaten') }} as district_key,
            {{ profile_whatsapp_key('whatsapp') }} as whatsapp_key
        from {{ ref('ka_merge_int') }}
        where is_latest and date >= date '2026-04-01'
    ) src
),

-- Normalize canonical profile identifiers.
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

-- Build the profile join key.
profile_lookup as (
    select
        *,
        {{ profile_person_key('name_key', 'email', 'whatsapp_key', 'district_key') }} as profile_join_key
    from profile_lookup_base
),

-- Attach canonical profiles to current KA records.
matched_profiles as (
    select
        year,
        quarter,
        date,
        cr.nama,
        cr.email,
        cr.age,
        cr.education,
        cr.program,
        cr.role,
        cr.desa_kelurahan,
        cr.is_certified,
        cr.kota_kabupaten,
        cr.modul,
        cr.whatsapp,
        cr.puskesmas,
        cr.is_latest,
        cr.score,
        cr.canonical_nama,
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
    nama,
    email,
    age,
    education,
    program,
    role,
    desa_kelurahan,
    is_certified,
    kota_kabupaten,
    modul,
    whatsapp,
    puskesmas,
    is_latest,
    score,
    coalesce(person_id, concat(coalesce(email, whatsapp_key, name_key, 'unknown_person'), '_c0')) as person_id,
    coalesce(profile_name, canonical_nama, nama) as canonical_nama
from matched_profiles
