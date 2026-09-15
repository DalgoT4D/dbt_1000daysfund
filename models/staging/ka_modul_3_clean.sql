{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["ka"]
) }}

-- Load raw KA module 3 records from both response sheets.
with source as (
    select
        "Timestamp",
        "Nama",
        "Email_Address",
        "Peran_Anda",
        "Nomor_HP_WA",
        "Kabupaten_Kota",
        "Provinsi",
        "Puskesmas",
        "Desa_Kelurahan",
        "Score"
    from {{ source('raw_sheets', 'ka_modul_03') }}

    union all

    select
        "Timestamp",
        "Nama",
        "Email_Address",
        "Peran_Anda",
        "Nomor_HP_WA",
        "Kabupaten_Kota",
        "Provinsi",
        "Puskesmas",
        "Desa_Kelurahan",
        "Score"
    from {{ source('raw_sheets', 'ka_sp_modul_03') }}
),

-- Load approved district-name corrections.
district_lookup as (
    select
        typo_key,
        kota_kabupaten
    from {{ ref('ka_district_lookup_int') }}
),

-- Parse and normalize attendance fields.
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
        "Nama" as nama,
        {{ normalize_unicode('"Nama"') }} as clean_nama,
        lower(trim("Email_Address")) as email,
        trim("Peran_Anda") as role_raw,
        "Nomor_HP_WA" as whatsapp,
        {{ normalize_unicode('"Kabupaten_Kota"') }} as kota_kabupaten_raw,
        trim("Provinsi") as provinsi,
        trim("Puskesmas") as puskesmas,
        trim("Desa_Kelurahan") as desa_kelurahan,
        round(
            (
                cast(trim(split_part("Score", '/', 1)) as numeric)
                / nullif(cast(trim(split_part("Score", '/', 2)) as numeric), 0)
            ) * 100,
            2
        ) as score
    from source
),

-- Keep records with valid training dates.
filtered_dates as (
    select *
    from parsed p
    where date >= date '2026-07-01'
),

-- Apply approved district-name corrections.
district_corrected as (
    select
        p.*,
        coalesce(dl.kota_kabupaten, p.kota_kabupaten_raw) as kota_kabupaten,
        case
            when lower(coalesce(p.role_raw, '')) = 'tenaga kesehatan' then 'Health Worker'
            when lower(coalesce(p.role_raw, '')) = 'kader posyandu' then 'Community Health Worker'
            when lower(coalesce(p.role_raw, '')) = 'mahasiswa' then 'Student'
            else 'Public'
        end as role
    from filtered_dates p
    left join district_lookup dl
        on p.kota_kabupaten_raw = dl.typo_key
)

select
    email,
    nama,
    clean_nama,
    role,
    whatsapp,
    kota_kabupaten_raw as kota_kabupaten_original,
    kota_kabupaten,
    provinsi,
    puskesmas,
    desa_kelurahan,
    year,
    quarter,
    date,
    timestamp_raw,
    score
from district_corrected