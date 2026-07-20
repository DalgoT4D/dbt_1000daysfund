{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["training_data_stg", "staging", "training"]
) }}

with training_01 as (
    select
        'Pelatihan ASI'::text  as training_type,
        '01'::text             as training_code,
        *
    from {{ source('raw_sheets', 'training_01_participants') }}
),

training_02 as (
    select
        'Growth Counseling'::text  as training_type,
        '02'::text                 as training_code,
        *
    from {{ source('raw_sheets', 'training_02_participants') }}
),

training_03 as (
    select
        'Fasilitasi Kelas Ibu'::text  as training_type,
        '03'::text                    as training_code,
        *
    from {{ source('raw_sheets', 'training_03_participants') }}
),

training_04 as (
    select
        'Stunting 101'::text  as training_type,
        '04'::text            as training_code,
        *
    from {{ source('raw_sheets', 'training_04_participants') }}
),

training_05a as (
    select
        'Konseling Nutrisi'::text  as training_type,
        '05a'::text                as training_code,
        *
    from {{ source('raw_sheets', 'training_05a_participants') }}
),

training_05b as (
    select
        'Konseling Nutrisi'::text  as training_type,
        '05b'::text                as training_code,
        *
    from {{ source('raw_sheets', 'training_05b_participants') }}
),

training_06 as (
    select
        'Manajemen Posyandu'::text  as training_type,
        '06'::text                  as training_code,
        *
    from {{ source('raw_sheets', 'training_06_participants') }}
),

training_07 as (
    select
        'Fasilitasi dan Komunikasi'::text  as training_type,
        '07'::text                         as training_code,
        *
    from {{ source('raw_sheets', 'training_07_participants') }}
),

training_09a as (
    select
        'ICCM'::text  as training_type,
        '09a'::text   as training_code,
        *
    from {{ source('raw_sheets', 'training_09a_participants') }}
),

training_09b as (
    select
        'ICCM'::text  as training_type,
        '09b'::text   as training_code,
        *
    from {{ source('raw_sheets', 'training_09b_participants') }}
),

training_10 as (
    select
        'ASI-MPASI'::text  as training_type,
        '10'::text         as training_code,
        *
    from {{ source('raw_sheets', 'training_10_participants') }}
),

training_11 as (
    select
        'Konseling Ibu Hamil'::text  as training_type,
        '11'::text                   as training_code,
        *
    from {{ source('raw_sheets', 'training_11_participants') }}
),

combined as (
    select * from training_01
    union all select * from training_02
    union all select * from training_03
    union all select * from training_04
    union all select * from training_05a
    union all select * from training_05b
    union all select * from training_06
    union all select * from training_07
    union all select * from training_09a
    union all select * from training_09b
    union all select * from training_10
    union all select * from training_11
)

select
    training_type,
    training_code,

    nullif(btrim("year"), '')::int            as year,
    nullif(btrim("quarter"), '')              as quarter,
    nullif(btrim("date"), '')::date           as date,
    nullif(btrim("status"), '')               as status,
    nullif(btrim("nama"), '')                 as name,
    nullif(btrim("peran_category"), '')       as peran_category,
    nullif(btrim("provinsi"), '')             as province,
    nullif(btrim("kabupaten"), '')            as district,
    nullif(btrim("kecamatan"), '')            as subdistrict,
    nullif(btrim("desa"), '')                 as village,
    nullif(btrim("pre_score"), '')::numeric   as pre_score,
    nullif(btrim("post_score"), '')::numeric  as post_score,
    nullif(btrim("delta"), '')::numeric       as delta,
    nullif(btrim("outcome"), '')              as outcome

from combined

union all

select
    'Kelompok Kerja'::text as training_type,
    '12'::text as training_code,
    year,
    quarter,
    date,
    status,
    nama as name,
    peran_category,
    provinsi as province,
    kabupaten as district,
    kecamatan as subdistrict,
    desa as village,
    pre_score::numeric,
    post_score::numeric,
    delta::numeric,
    outcome
from {{ ref('training_12_participants') }}

union all

select
    'Hari 1'::text as training_type,
    '13'::text as training_code,
    year,
    quarter,
    date,
    status,
    nama as name,
    peran_category,
    provinsi as province,
    kabupaten as district,
    kecamatan as subdistrict,
    desa as village,
    pre_score::numeric,
    post_score::numeric,
    delta::numeric,
    outcome
from {{ ref('training_13_participants') }}

union all

select
    'Hari 2'::text as training_type,
    '14'::text as training_code,
    year,
    quarter,
    date,
    status,
    nama as name,
    peran_category,
    provinsi as province,
    kabupaten as district,
    kecamatan as subdistrict,
    desa as village,
    pre_score::numeric,
    post_score::numeric,
    delta::numeric,
    outcome
from {{ ref('training_14_participants') }}
