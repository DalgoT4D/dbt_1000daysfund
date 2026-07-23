{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["ka", "intermediate"]
) }}

with past_profiles as (
    select
        nullif(trim("name"), '') as name,
        lower(nullif(trim("email"), '')) as email,
        nullif(trim("whatsapp"), '') as whatsapp,
        nullif(trim("role"), '') as role,
        nullif(trim("district"), '') as district,
        cast(nullif(trim("date"), '') as date) as date,
        cast(null as timestamp) as timestamp_raw,
        cast(null as varchar) as unified_name,
        {{ profile_name_key('"name"') }} as profile_name_key,
        {{ profile_name_key('"district"') }} as district_key,
        {{ profile_whatsapp_key('"whatsapp"') }} as whatsapp_key,
        'past' as profile_period
    from {{ source('raw_sheets', 'ka_past_quarter') }}
    where cast(nullif(trim("date"), '') as date) < date '2026-04-01'
),

current_profiles as (
    select
        nullif(trim(name), '') as name,
        lower(nullif(trim(email), '')) as email,
        nullif(trim(whatsapp), '') as whatsapp,
        nullif(trim(role), '') as role,
        nullif(trim(district), '') as district,
        date,
        date::timestamp as timestamp_raw,
        nullif(trim(unified_name), '') as unified_name,
        {{ profile_name_key('coalesce(unified_name, name)') }} as profile_name_key,
        {{ profile_name_key('district') }} as district_key,
        {{ profile_whatsapp_key('whatsapp') }} as whatsapp_key,
        'current' as profile_period
    from {{ ref('ka_merge_int') }}
    where date >= date '2026-04-01'
),

all_profiles as (
    select
        coalesce(unified_name, name) as canonical_name,
        {{ profile_person_key('profile_name_key', 'email', 'whatsapp_key', 'district_key') }} as person_key,
        name,
        email,
        whatsapp,
        role,
        district,
        date,
        timestamp_raw,
        unified_name,
        profile_period
    from past_profiles

    union all

    select
        coalesce(unified_name, name) as canonical_name,
        {{ profile_person_key('profile_name_key', 'email', 'whatsapp_key', 'district_key') }} as person_key,
        name,
        email,
        whatsapp,
        role,
        district,
        date,
        timestamp_raw,
        unified_name,
        profile_period
    from current_profiles
),

current_name_variants as (
    select
        person_key,
        string_agg(name_variant, ' | ' order by name_variant) as name_variants
    from (
        select distinct
            person_key,
            trim(name) as name_variant
        from all_profiles
        where profile_period = 'current'
            and person_key is not null
            and nullif(trim(name), '') is not null
    ) variants
    group by person_key
),

profile_rollup as (
    select
        person_key,
        (array_agg(unified_name order by date desc nulls last, timestamp_raw desc nulls last) filter (where profile_period = 'current' and unified_name is not null))[1] as current_unified_name,
        (array_agg(name order by date desc nulls last, timestamp_raw desc nulls last) filter (where name is not null))[1] as latest_name,
        (array_agg(email order by date desc nulls last, timestamp_raw desc nulls last) filter (where email is not null))[1] as email,
        (array_agg(whatsapp order by date desc nulls last, timestamp_raw desc nulls last) filter (where whatsapp is not null))[1] as whatsapp,
        (array_agg(role order by date desc nulls last, timestamp_raw desc nulls last) filter (where role is not null))[1] as role,
        (array_agg(district order by date desc nulls last, timestamp_raw desc nulls last) filter (where district is not null))[1] as district
    from all_profiles
    where person_key is not null
    group by person_key
),

profiles_with_variants as (
    select
        pr.person_key,
        coalesce(pr.current_unified_name, pr.latest_name) as canonical_name,
        pr.email,
        pr.whatsapp,
        pr.role,
        pr.district,
        cnv.name_variants
    from profile_rollup pr
    left join current_name_variants cnv
        on pr.person_key = cnv.person_key
),

profiles_ranked as (
    select
        *,
        {{ profile_whatsapp_key('whatsapp') }} as whatsapp_key,
        {{ profile_name_key('canonical_name') }} as canonical_name_key,
        row_number() over (
            partition by coalesce(email, {{ profile_whatsapp_key('whatsapp') }}, {{ profile_name_key('canonical_name') }}, md5(person_key))
            order by canonical_name, district, whatsapp
        ) - 1 as cluster_index
    from profiles_with_variants
)

select
    concat(coalesce(email, whatsapp_key, canonical_name_key, md5(person_key)), '_c', cluster_index) as person_id,
    canonical_name as name,
    email,
    whatsapp,
    role,
    district,
    name_variants
from profiles_ranked
where canonical_name is not null
