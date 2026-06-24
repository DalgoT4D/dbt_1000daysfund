{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["ka"]
) }}

with past_quarter_source as (
    select * from {{ source('raw_sheets', 'ka_past_quarter') }}
),

past_quarter_normalized as (
    select
        cast(nullif(trim("year"), '') as integer) as year,
        nullif(trim("quarter"), '') as quarter,
        cast(nullif(trim("date"), '') as date) as date,
        nullif(trim("name"), '') as name,
        nullif(trim("email"), '') as email,
        nullif(trim("role"), '') as role,
        nullif(trim("whatsapp"), '') as whatsapp,
        nullif(trim("district"), '') as district,
        nullif(trim("puskesmas"), '') as puskesmas,
        nullif(trim("village"), '') as village,
        cast(nullif(trim("score"), '') as numeric) as score_raw,
        nullif(trim("modul"), '') as modul,
        nullif(trim("program"), '') as program
    from past_quarter_source
    -- where nullif(trim("quarter"), '') not in ('2026-Q1', '2026-Q2')
    where cast(nullif(trim("date"), '') as date) < date '2026-01-01'
),

past_quarter as (
    select
        email,
        name,
        cast(null as varchar) as unified_name,
        role,
        whatsapp,
        district,
        cast(null as varchar) as province,
        puskesmas,
        village,
        year,
        quarter,
        date,
        cast(round(score_raw, 0) as integer) as score,
        case
            when score_raw >= 80 then 'TRUE'
            when score_raw is not null then 'FALSE'
            else null
        end as is_certified,
        modul,
        program
    from past_quarter_normalized
),

current_modules_raw as (
    select
        email,
        name,
        unified_name,
        role,
        whatsapp,
        district,
        province,
        puskesmas,
        village,
        cast(year as integer) as year,
        quarter,
        date,
        score,
        'modul_1' as modul
    from {{ ref('ka_modul_1_clean') }}

    union all

    select
        email,
        name,
        unified_name,
        role,
        whatsapp,
        district,
        province,
        puskesmas,
        village,
        cast(year as integer) as year,
        quarter,
        date,
        score,
        'modul_2' as modul
    from {{ ref('ka_modul_2_clean') }}

    union all

    select
        email,
        name,
        unified_name,
        role,
        whatsapp,
        district,
        province,
        puskesmas,
        village,
        cast(year as integer) as year,
        quarter,
        date,
        score,
        'modul_3' as modul
    from {{ ref('ka_modul_3_clean') }}
),

current_modules as (
    select
        email,
        name,
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
        cast(round(score, 0) as integer) as score,
        case
            when score >= 80 then 'TRUE'
            when score is not null then 'FALSE'
            else null
        end as is_certified,
        modul,
        cast(null as varchar) as program
    from current_modules_raw
)

select *
from past_quarter

union all

select *
from current_modules
