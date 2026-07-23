{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["epus_baduta_stg", "staging"]
) }}

-- EPUS baduta, already processed upstream (carries age, z-scores, and flags).
-- This model only RESHAPES it to match register_posyandu_baduta_stg's output:
-- drops the _airbyte_* metadata, casts to the target types, and derives
-- year/quarter. No recomputation. The *_biner columns stay text ('TRUE'/'FALSE')
-- to match the register baduta staging output. Source assumed airbyte-typed (real NULLs).

with source as (

    select * from {{ source('raw_sheets', 'epus_baduta') }}

),

typed as (

    select
        kunjungan_tanggal::date             as kunjungan_tanggal,

        chw_nama_1, chw_nama_2, chw_nama_3, chw_nama_4, chw_nama_5, chw_nama_6,
        hw_nama_1, hw_nama_2, hw_nama_3, hw_nama_4, hw_nama_5,
        provinsi, kota_kabupaten, kecamatan, desa_kelurahan, puskesmas, posyandu,

        baduta_nama,
        baduta_gender,
        baduta_tanggal_lahir::date          as baduta_tanggal_lahir,
        baduta_usia_bulan::int              as baduta_usia_bulan,
        baduta_pengasuh_nama,
        baduta_asi_biner,
        baduta_mpasi_biner,
        baduta_protein_biner,
        baduta_berat_badan::numeric         as baduta_berat_badan,
        baduta_tinggi_badan::numeric        as baduta_tinggi_badan,
        kr_jumlah::int                      as kr_jumlah,
        catatan,

        baduta_waz::numeric                 as baduta_waz,
        baduta_haz::numeric                 as baduta_haz,
        baduta_waz_check::boolean           as baduta_waz_check,
        baduta_haz_check::boolean           as baduta_haz_check,
        baduta_asi_check::boolean           as baduta_asi_check,
        baduta_protein_check::boolean       as baduta_protein_check,
        baduta_wf_check::boolean            as baduta_wf_check

    from source

)

select
    kunjungan_tanggal,
    extract(year from kunjungan_tanggal)::int as year,
    extract(year from kunjungan_tanggal)::int || '-Q' || extract(quarter from kunjungan_tanggal)::int as quarter,
    chw_nama_1, chw_nama_2, chw_nama_3, chw_nama_4, chw_nama_5, chw_nama_6,
    hw_nama_1, hw_nama_2, hw_nama_3, hw_nama_4, hw_nama_5,
    provinsi, kota_kabupaten, kecamatan, desa_kelurahan, puskesmas, posyandu,
    baduta_nama,
    baduta_gender,
    baduta_tanggal_lahir,
    baduta_usia_bulan,
    baduta_pengasuh_nama,
    baduta_asi_biner,
    baduta_mpasi_biner,
    baduta_protein_biner,
    baduta_berat_badan,
    baduta_tinggi_badan,
    kr_jumlah,
    catatan,
    baduta_waz,
    baduta_haz,
    baduta_waz_check,
    baduta_haz_check,
    baduta_asi_check,
    baduta_protein_check,
    baduta_wf_check
from typed