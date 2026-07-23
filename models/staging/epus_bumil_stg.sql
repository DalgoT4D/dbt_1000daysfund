{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["epus_bumil_stg", "staging"]
) }}

-- EPUS bumil, already processed upstream (carries derived + check columns).
-- This model only RESHAPES it to match register_posyandu_bumil_stg's output:
-- drops the _airbyte_* metadata, casts to the target types, and derives
-- year/quarter. No recomputation of checks/derivations. Source table is
-- assumed to use real NULLs (airbyte-typed), not empty strings.

with source as (

    select * from {{ source('raw_sheets', 'epus_bumil') }}

),

typed as (

    select
        kunjungan_tanggal::date                     as kunjungan_tanggal,

        chw_nama_1, chw_nama_2, chw_nama_3, chw_nama_4, chw_nama_5, chw_nama_6,
        hw_nama_1, hw_nama_2, hw_nama_3, hw_nama_4, hw_nama_5,
        provinsi, kota_kabupaten, kecamatan, desa_kelurahan, puskesmas, posyandu,

        ibu_nama,
        ibu_usia::int                               as ibu_usia,
        ibu_hpht::date                              as ibu_hpht,
        ibu_tekanan_darah_sistolik::numeric         as ibu_tekanan_darah_sistolik,
        ibu_tekanan_darah_diastolik::numeric        as ibu_tekanan_darah_diastolik,
        ibu_lila::numeric                           as ibu_lila,
        ibu_ttd_mulai::numeric                      as ibu_ttd_mulai,
        ibu_ttd_jumlah_raw::numeric                 as ibu_ttd_jumlah_raw,
        ibu_anc_jumlah::int                         as ibu_anc_jumlah,
        kr_jumlah::int                              as kr_jumlah,
        catatan,

        ibu_usia_kehamilan_minggu::int              as ibu_usia_kehamilan_minggu,
        ibu_usia_kehamilan_hari::int                as ibu_usia_kehamilan_hari,
        ibu_trimester::int                          as ibu_trimester,
        ibu_ttd_perc::numeric                       as ibu_ttd_perc,
        ibu_kek_check::boolean                      as ibu_kek_check,
        ibu_anc_check::boolean                      as ibu_anc_check,
        ibu_hipertensi_check::boolean               as ibu_hipertensi_check,
        ibu_risti_check::boolean                    as ibu_risti_check,
        ibu_ttd_check::boolean                      as ibu_ttd_check

    from source

)

select
    kunjungan_tanggal,
    extract(year from kunjungan_tanggal)::int as year,
    extract(year from kunjungan_tanggal)::int || '-Q' || extract(quarter from kunjungan_tanggal)::int as quarter,
    chw_nama_1, chw_nama_2, chw_nama_3, chw_nama_4, chw_nama_5, chw_nama_6,
    hw_nama_1, hw_nama_2, hw_nama_3, hw_nama_4, hw_nama_5,
    provinsi, kota_kabupaten, kecamatan, desa_kelurahan, puskesmas, posyandu,
    ibu_nama,
    ibu_usia,
    ibu_hpht,
    ibu_tekanan_darah_sistolik,
    ibu_tekanan_darah_diastolik,
    ibu_lila,
    ibu_ttd_mulai,
    ibu_ttd_jumlah_raw,
    ibu_anc_jumlah,
    kr_jumlah,
    catatan,
    ibu_usia_kehamilan_minggu,
    ibu_usia_kehamilan_hari,
    ibu_trimester,
    ibu_ttd_perc,
    ibu_kek_check,
    ibu_anc_check,
    ibu_hipertensi_check,
    ibu_risti_check,
    ibu_ttd_check
from typed