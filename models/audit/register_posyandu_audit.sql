-- Model: Combines maternal and child Posyandu records from all sources.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["register_posyandu", "audit"]
) }}

with

tdf_baduta as (
    select
        'tdf' as source,
        'baduta' as data_type,
        kota_kabupaten as kota_kabupaten,
        baduta_nama as nama,
        source_row as row,
        fail_reasons as fail_reasons
    from {{ ref('register_posyandu_tdf_baduta_audit') }}

),

tdf_bumil as (
    select
        'tdf' as source,
        'bumil' as data_type,
        kota_kabupaten as kota_kabupaten,
        ibu_nama as nama,
        source_row as row,
        fail_reasons as fail_reasons
    from {{ ref('register_posyandu_tdf_bumil_audit') }}

),

pemerintah_baduta as (
    select
        'pemerintah' as source,
        'baduta' as data_type,
        kota_kabupaten as kota_kabupaten,
        baduta_nama as nama,
        source_row as row,
        fail_reasons as fail_reasons
    from {{ ref('register_posyandu_pemerintah_baduta_audit') }}

),

pemerintah_bumil as (
    select
        'pemerintah' as source,
        'bumil' as data_type,
        kota_kabupaten as kota_kabupaten,
        ibu_nama as nama,
        source_row as row,
        fail_reasons as fail_reasons
    from {{ ref('register_posyandu_pemerintah_bumil_audit') }}

),

unioned as(
    select * from tdf_baduta
    union all
    select * from tdf_bumil
    union all
    select * from pemerintah_baduta
    union all
    select * from pemerintah_bumil
)

select * from unioned