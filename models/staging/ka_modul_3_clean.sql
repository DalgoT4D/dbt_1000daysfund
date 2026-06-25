{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["ka"]
) }}

with source as (
    select * from {{ source('raw_sheets', 'ka_modul_03') }}
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
     where date >= date '2026-04-01'
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
)

select
    email,
    name,
    clean_name,
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
from district_corrected
