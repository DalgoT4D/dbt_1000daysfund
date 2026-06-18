{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["ka"]
) }}

with source as (
    select * from {{ source('raw_sheets', 'ka_modul_02') }}
),

district_typos as (
    select typo, district from {{ ref('district_typos') }}
),

parsed as (
    select
        cast("Timestamp" as timestamp) as timestamp_raw,
        cast(cast("Timestamp" as timestamp) as date) as date,
        extract(year from cast("Timestamp" as timestamp)) as year,
        concat(
            cast(extract(year from cast("Timestamp" as timestamp)) as varchar),
            '-Q',
            cast(extract(quarter from cast("Timestamp" as timestamp)) as varchar)
        ) as quarter,
        "Nama" as name,
        {{ normalize_unicode('"Nama"') }} as clean_name,
        lower(trim("Email_Address")) as email,
        trim("Peran_Anda") as role_raw,
        "Nomor_HP_WA" as whatsapp,
        {{ normalize_unicode('"Kabupaten_Kota"') }} as district_raw,
        trim("Provinsi") as province,
        trim("Puskesmas") as puskesmas,
        trim("Desa_Kelurahan") as village,
        round(
            (
                cast(trim(split_part("Score", '/', 1)) as numeric)
                / nullif(cast(trim(split_part("Score", '/', 2)) as numeric), 0)
            ) * 100,
            2
        ) as score
    from source
),

filtered_dates as (
    select *
    from parsed p
    where p.date >= date '2026-01-01'
),

district_corrected as (
    select
        p.*,
        coalesce(nullif(dt.district, ''), p.district_raw) as district,
        case
            when lower(coalesce(p.role_raw, '')) = 'tenaga kesehatan' then 'Health Worker'
            when lower(coalesce(p.role_raw, '')) = 'kader posyandu' then 'Community Health Worker'
            when lower(coalesce(p.role_raw, '')) = 'mahasiswa' then 'Student'
            else 'Public'
        end as role
    from filtered_dates p
    left join district_typos dt
        on p.district_raw = lower(trim(dt.typo))
),

name_match_candidates as (
    select
        a.email,
        a.clean_name,
        b.clean_name as matched_clean_name,
        regexp_replace(a.clean_name, '[^a-z ]', '', 'g') as a_name_plain,
        regexp_replace(b.clean_name, '[^a-z ]', '', 'g') as b_name_plain,
        similarity(
            regexp_replace(a.clean_name, '[^a-z ]', '', 'g'),
            regexp_replace(b.clean_name, '[^a-z ]', '', 'g')
        ) as clean_name_similarity
    from district_corrected a
    join district_corrected b
        on a.email = b.email
        and a.clean_name <> b.clean_name
        and regexp_replace(a.clean_name, '[^a-z ]', '', 'g')
            % regexp_replace(b.clean_name, '[^a-z ]', '', 'g')
    where a.clean_name is not null
        and b.clean_name is not null
),

name_groups as (
    select
        email,
        clean_name,
        min(matched_clean_name) as name_group_key
    from name_match_candidates
    where clean_name_similarity >= 0.64
        or (
            regexp_replace(a_name_plain, '^.* ', '') = regexp_replace(b_name_plain, '^.* ', '')
            and similarity(
                split_part(a_name_plain, ' ', 1),
                split_part(b_name_plain, ' ', 1)
            ) >= 0.5
        )
    group by email, clean_name
),

with_unified as (
    select
        d.email,
        d.name,
        d.clean_name,
        d.role,
        d.whatsapp,
        first_value(d.name) over (
            partition by d.email, coalesce(ng.name_group_key, d.clean_name)
            order by length(d.clean_name) desc, d.timestamp_raw asc
            rows between unbounded preceding and unbounded following
        ) as unified_name,
        d.district,
        d.province,
        d.puskesmas,
        d.village,
        d.year,
        d.quarter,
        d.date,
        d.timestamp_raw,
        d.score
    from district_corrected d
    left join name_groups ng
        on d.email = ng.email
        and d.clean_name = ng.clean_name
),


ranked as (
    select
        *,
        row_number() over (
            partition by quarter, email, unified_name
            order by timestamp_raw desc
        ) as rn
    from with_unified
)

select
    email,
    name,
    clean_name,
    unified_name,
    role,
    whatsapp,
    district,
    province,
    puskesmas,
    village,
    year,
    quarter,
    date,
    timestamp_raw,
    score
from ranked
where rn = 1
