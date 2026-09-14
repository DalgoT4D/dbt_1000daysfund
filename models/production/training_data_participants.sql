-- Model: Combines participant outcomes across all training cohorts.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["training_data_participants", "staging", "training"]
) }}

select
    training_type,
    training_code,
    year,
    quarter,
    date,
    status,
    nama as name,
    peran_category,
    provinsi,
    kota_kabupaten,
    kecamatan,
    desa_kelurahan,
    pre_score::numeric,
    post_score::numeric,
    delta::numeric,
    outcome
from {{ ref('training_old_participants_stg') }}

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
    provinsi as provinsi,
    kabupaten as kota_kabupaten,
    kecamatan as kecamatan,
    desa as desa_kelurahan,
    pre_score::numeric,
    post_score::numeric,
    delta::numeric,
    outcome
from {{ ref('training_12_participants') }}

union all

select
    'Poster Pintar, GC, Manajemen Posyandu'::text as training_type,
    '13'::text as training_code,
    year,
    quarter,
    date,
    status,
    nama as name,
    peran_category,
    provinsi as provinsi,
    kabupaten as kota_kabupaten,
    kecamatan as kecamatan,
    desa as desa_kelurahan,
    pre_score::numeric,
    post_score::numeric,
    delta::numeric,
    outcome
from {{ ref('training_13_participants') }}

union all

select
    'ASI, MPASI, ICCM'::text as training_type,
    '14'::text as training_code,
    year,
    quarter,
    date,
    status,
    nama as name,
    peran_category,
    provinsi as provinsi,
    kabupaten as kota_kabupaten,
    kecamatan as kecamatan,
    desa as desa_kelurahan,
    pre_score::numeric,
    post_score::numeric,
    delta::numeric,
    outcome
from {{ ref('training_14_participants') }}
