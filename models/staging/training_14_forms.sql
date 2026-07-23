{{ config(materialized='table', tags=['staging', 'training_14', 'training']) }}

select
    'forms:' || record_id as record_id, 'forms'::text as response_source,
    form_tag, source_row_id, nama_raw, unified_name, unified_name_key,
    desa_raw, cast(usia as text) as usia, peran_raw, score::numeric as score,
    provinsi_raw, kabupaten_raw, kecamatan_raw, puskesmas_raw, posyandu_raw,
    timestamp_raw, year, quarter, phone_raw, phone_key, jenis_kelamin_raw,
    jenis_kelamin, null::text as nik_raw, null::text as nik_key,
    education_raw, education, peran_category, nama_key, desa_key,
    kabupaten_key, kecamatan_key, puskesmas_key
from {{ ref('training_14_forms_stg') }}

union all

select
    'sheets:' || record_id, 'sheets'::text,
    form_tag, source_row_id, nama_raw, unified_name, unified_name_key,
    desa_raw, cast(usia as text), peran_raw, score::numeric,
    provinsi_raw, kabupaten_raw, kecamatan_raw, puskesmas_raw, posyandu_raw,
    training_date::timestamp, year, quarter, phone_raw, phone_key,
    jenis_kelamin_raw, jenis_kelamin, null::text, null::text,
    education_raw, education, peran_category, nama_key, desa_key,
    kabupaten_key, kecamatan_key, puskesmas_key
from {{ ref('training_14_sheets_stg') }}
