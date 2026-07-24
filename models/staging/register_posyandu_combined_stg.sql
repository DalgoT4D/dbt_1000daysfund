{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["register_posyandu_combined_stg", "staging", "register_posyandu", "parent"]
) }}

-- Unified Posyandu register: UNION ALL of four staging tables across two sources.
--   responden_kategori : 'bumil' | 'baduta'
--   source             : 'register_posyandu' | 'epus'
-- The two sources are disjoint (no shared visits), so UNION ALL is safe.
-- Each branch selects its columns BY NAME, fills its own program's columns and
-- typed-NULLs the other's. Column order is identical across all four branches.
-- A final wrapper derives kasus_check / kasus from the four condition flags.

with

reg_bumil as (

    select
        'bumil'                          as responden_kategori,
        'register_posyandu'              as source,
        kunjungan_tanggal                as kunjungan_tanggal,
        extract(year from kunjungan_tanggal)::int as year,
        extract(year from kunjungan_tanggal)::int || '-Q' || extract(quarter from kunjungan_tanggal)::int as quarter,
        chw_nama_1                       as chw_nama_1,
        chw_nama_2                       as chw_nama_2,
        chw_nama_3                       as chw_nama_3,
        chw_nama_4                       as chw_nama_4,
        chw_nama_5                       as chw_nama_5,
        chw_nama_6                       as chw_nama_6,
        hw_nama_1                        as hw_nama_1,
        hw_nama_2                        as hw_nama_2,
        hw_nama_3                        as hw_nama_3,
        hw_nama_4                        as hw_nama_4,
        hw_nama_5                        as hw_nama_5,
        provinsi                         as provinsi,
        kota_kabupaten                   as kota_kabupaten,
        kecamatan                        as kecamatan,
        desa_kelurahan                   as desa_kelurahan,
        puskesmas                        as puskesmas,
        posyandu                         as posyandu,
        kr_jumlah                        as kr_jumlah,
        catatan                          as catatan,
        ibu_nama                         as ibu_nama,
        ibu_usia                         as ibu_usia,
        ibu_hpht                         as ibu_hpht,
        ibu_tekanan_darah_sistolik       as ibu_tekanan_darah_sistolik,
        ibu_tekanan_darah_diastolik      as ibu_tekanan_darah_diastolik,
        ibu_lila                         as ibu_lila,
        ibu_ttd_mulai                    as ibu_ttd_mulai,
        ibu_ttd_jumlah_raw               as ibu_ttd_jumlah_raw,
        ibu_anc_jumlah                   as ibu_anc_jumlah,
        ibu_usia_kehamilan_minggu        as ibu_usia_kehamilan_minggu,
        ibu_usia_kehamilan_hari          as ibu_usia_kehamilan_hari,
        ibu_trimester                    as ibu_trimester,
        ibu_ttd_perc                     as ibu_ttd_perc,
        ibu_kek_check                    as ibu_kek_check,
        ibu_anc_check                    as ibu_anc_check,
        ibu_hipertensi_check             as ibu_hipertensi_check,
        ibu_risti_check                  as ibu_risti_check,
        ibu_ttd_check                    as ibu_ttd_check,
        cast(null as text    ) as baduta_nama,
        cast(null as text    ) as baduta_gender,
        cast(null as date    ) as baduta_tanggal_lahir,
        cast(null as integer ) as baduta_usia_bulan,
        cast(null as text    ) as baduta_pengasuh_nama,
        cast(null as text    ) as baduta_asi_biner,
        cast(null as text    ) as baduta_mpasi_biner,
        cast(null as text    ) as baduta_protein_biner,
        cast(null as numeric ) as baduta_berat_badan,
        cast(null as numeric ) as baduta_tinggi_badan,
        cast(null as numeric ) as baduta_waz,
        cast(null as numeric ) as baduta_haz,
        cast(null as boolean ) as baduta_waz_check,
        cast(null as boolean ) as baduta_haz_check,
        cast(null as boolean ) as baduta_asi_check,
        cast(null as boolean ) as baduta_protein_check,
        cast(null as boolean ) as baduta_wf_check
    from {{ ref('register_posyandu_bumil_stg') }}

),

reg_baduta as (

    select
        'baduta'                         as responden_kategori,
        'register_posyandu'              as source,
        kunjungan_tanggal                as kunjungan_tanggal,
        extract(year from kunjungan_tanggal)::int as year,
        extract(year from kunjungan_tanggal)::int || '-Q' || extract(quarter from kunjungan_tanggal)::int as quarter,
        chw_nama_1                       as chw_nama_1,
        chw_nama_2                       as chw_nama_2,
        chw_nama_3                       as chw_nama_3,
        chw_nama_4                       as chw_nama_4,
        chw_nama_5                       as chw_nama_5,
        chw_nama_6                       as chw_nama_6,
        hw_nama_1                        as hw_nama_1,
        hw_nama_2                        as hw_nama_2,
        hw_nama_3                        as hw_nama_3,
        hw_nama_4                        as hw_nama_4,
        hw_nama_5                        as hw_nama_5,
        provinsi                         as provinsi,
        kota_kabupaten                   as kota_kabupaten,
        kecamatan                        as kecamatan,
        desa_kelurahan                   as desa_kelurahan,
        puskesmas                        as puskesmas,
        posyandu                         as posyandu,
        kr_jumlah                        as kr_jumlah,
        catatan                          as catatan,
        cast(null as text    ) as ibu_nama,
        cast(null as integer ) as ibu_usia,
        cast(null as date    ) as ibu_hpht,
        cast(null as numeric ) as ibu_tekanan_darah_sistolik,
        cast(null as numeric ) as ibu_tekanan_darah_diastolik,
        cast(null as numeric ) as ibu_lila,
        cast(null as numeric ) as ibu_ttd_mulai,
        cast(null as numeric ) as ibu_ttd_jumlah_raw,
        cast(null as integer ) as ibu_anc_jumlah,
        cast(null as integer ) as ibu_usia_kehamilan_minggu,
        cast(null as integer ) as ibu_usia_kehamilan_hari,
        cast(null as integer ) as ibu_trimester,
        cast(null as numeric ) as ibu_ttd_perc,
        cast(null as boolean ) as ibu_kek_check,
        cast(null as boolean ) as ibu_anc_check,
        cast(null as boolean ) as ibu_hipertensi_check,
        cast(null as boolean ) as ibu_risti_check,
        cast(null as boolean ) as ibu_ttd_check,
        baduta_nama                      as baduta_nama,
        baduta_gender                    as baduta_gender,
        baduta_tanggal_lahir             as baduta_tanggal_lahir,
        baduta_usia_bulan                as baduta_usia_bulan,
        baduta_pengasuh_nama             as baduta_pengasuh_nama,
        baduta_asi_biner                 as baduta_asi_biner,
        baduta_mpasi_biner               as baduta_mpasi_biner,
        baduta_protein_biner             as baduta_protein_biner,
        baduta_berat_badan               as baduta_berat_badan,
        baduta_tinggi_badan              as baduta_tinggi_badan,
        baduta_waz                       as baduta_waz,
        baduta_haz                       as baduta_haz,
        baduta_waz_check                 as baduta_waz_check,
        baduta_haz_check                 as baduta_haz_check,
        baduta_asi_check                 as baduta_asi_check,
        baduta_protein_check             as baduta_protein_check,
        cast(null as boolean ) as baduta_wf_check
    from {{ ref('register_posyandu_baduta_stg') }}

),

epus_bumil as (

    select
        'bumil'                          as responden_kategori,
        'epus'                           as source,
        kunjungan_tanggal                as kunjungan_tanggal,
        year                             as year,
        quarter                          as quarter,
        chw_nama_1                       as chw_nama_1,
        chw_nama_2                       as chw_nama_2,
        chw_nama_3                       as chw_nama_3,
        chw_nama_4                       as chw_nama_4,
        chw_nama_5                       as chw_nama_5,
        chw_nama_6                       as chw_nama_6,
        hw_nama_1                        as hw_nama_1,
        hw_nama_2                        as hw_nama_2,
        hw_nama_3                        as hw_nama_3,
        hw_nama_4                        as hw_nama_4,
        hw_nama_5                        as hw_nama_5,
        provinsi                         as provinsi,
        kota_kabupaten                   as kota_kabupaten,
        kecamatan                        as kecamatan,
        desa_kelurahan                   as desa_kelurahan,
        puskesmas                        as puskesmas,
        posyandu                         as posyandu,
        kr_jumlah                        as kr_jumlah,
        catatan                          as catatan,
        ibu_nama                         as ibu_nama,
        ibu_usia                         as ibu_usia,
        ibu_hpht                         as ibu_hpht,
        ibu_tekanan_darah_sistolik       as ibu_tekanan_darah_sistolik,
        ibu_tekanan_darah_diastolik      as ibu_tekanan_darah_diastolik,
        ibu_lila                         as ibu_lila,
        ibu_ttd_mulai                    as ibu_ttd_mulai,
        ibu_ttd_jumlah_raw               as ibu_ttd_jumlah_raw,
        ibu_anc_jumlah                   as ibu_anc_jumlah,
        ibu_usia_kehamilan_minggu        as ibu_usia_kehamilan_minggu,
        ibu_usia_kehamilan_hari          as ibu_usia_kehamilan_hari,
        ibu_trimester                    as ibu_trimester,
        ibu_ttd_perc                     as ibu_ttd_perc,
        ibu_kek_check                    as ibu_kek_check,
        ibu_anc_check                    as ibu_anc_check,
        ibu_hipertensi_check             as ibu_hipertensi_check,
        ibu_risti_check                  as ibu_risti_check,
        ibu_ttd_check                    as ibu_ttd_check,
        cast(null as text    ) as baduta_nama,
        cast(null as text    ) as baduta_gender,
        cast(null as date    ) as baduta_tanggal_lahir,
        cast(null as integer ) as baduta_usia_bulan,
        cast(null as text    ) as baduta_pengasuh_nama,
        cast(null as text    ) as baduta_asi_biner,
        cast(null as text    ) as baduta_mpasi_biner,
        cast(null as text    ) as baduta_protein_biner,
        cast(null as numeric ) as baduta_berat_badan,
        cast(null as numeric ) as baduta_tinggi_badan,
        cast(null as numeric ) as baduta_waz,
        cast(null as numeric ) as baduta_haz,
        cast(null as boolean ) as baduta_waz_check,
        cast(null as boolean ) as baduta_haz_check,
        cast(null as boolean ) as baduta_asi_check,
        cast(null as boolean ) as baduta_protein_check,
        cast(null as boolean ) as baduta_wf_check
    from {{ ref('epus_bumil_stg') }}

),

epus_baduta as (

    select
        'baduta'                         as responden_kategori,
        'epus'                           as source,
        kunjungan_tanggal                as kunjungan_tanggal,
        year                             as year,
        quarter                          as quarter,
        chw_nama_1                       as chw_nama_1,
        chw_nama_2                       as chw_nama_2,
        chw_nama_3                       as chw_nama_3,
        chw_nama_4                       as chw_nama_4,
        chw_nama_5                       as chw_nama_5,
        chw_nama_6                       as chw_nama_6,
        hw_nama_1                        as hw_nama_1,
        hw_nama_2                        as hw_nama_2,
        hw_nama_3                        as hw_nama_3,
        hw_nama_4                        as hw_nama_4,
        hw_nama_5                        as hw_nama_5,
        provinsi                         as provinsi,
        kota_kabupaten                   as kota_kabupaten,
        kecamatan                        as kecamatan,
        desa_kelurahan                   as desa_kelurahan,
        puskesmas                        as puskesmas,
        posyandu                         as posyandu,
        kr_jumlah                        as kr_jumlah,
        catatan                          as catatan,
        cast(null as text    ) as ibu_nama,
        cast(null as integer ) as ibu_usia,
        cast(null as date    ) as ibu_hpht,
        cast(null as numeric ) as ibu_tekanan_darah_sistolik,
        cast(null as numeric ) as ibu_tekanan_darah_diastolik,
        cast(null as numeric ) as ibu_lila,
        cast(null as numeric ) as ibu_ttd_mulai,
        cast(null as numeric ) as ibu_ttd_jumlah_raw,
        cast(null as integer ) as ibu_anc_jumlah,
        cast(null as integer ) as ibu_usia_kehamilan_minggu,
        cast(null as integer ) as ibu_usia_kehamilan_hari,
        cast(null as integer ) as ibu_trimester,
        cast(null as numeric ) as ibu_ttd_perc,
        cast(null as boolean ) as ibu_kek_check,
        cast(null as boolean ) as ibu_anc_check,
        cast(null as boolean ) as ibu_hipertensi_check,
        cast(null as boolean ) as ibu_risti_check,
        cast(null as boolean ) as ibu_ttd_check,
        baduta_nama                      as baduta_nama,
        baduta_gender                    as baduta_gender,
        baduta_tanggal_lahir             as baduta_tanggal_lahir,
        baduta_usia_bulan                as baduta_usia_bulan,
        baduta_pengasuh_nama             as baduta_pengasuh_nama,
        baduta_asi_biner                 as baduta_asi_biner,
        baduta_mpasi_biner               as baduta_mpasi_biner,
        baduta_protein_biner             as baduta_protein_biner,
        baduta_berat_badan               as baduta_berat_badan,
        baduta_tinggi_badan              as baduta_tinggi_badan,
        baduta_waz                       as baduta_waz,
        baduta_haz                       as baduta_haz,
        baduta_waz_check                 as baduta_waz_check,
        baduta_haz_check                 as baduta_haz_check,
        baduta_asi_check                 as baduta_asi_check,
        baduta_protein_check             as baduta_protein_check,
        cast(null as boolean ) as baduta_wf_check
    from {{ ref('epus_baduta_stg') }}

),

unioned as (

    select * from reg_bumil
    union all
    select * from reg_baduta
    union all
    select * from epus_bumil
    union all
    select * from epus_baduta

)

select
    *,

    -- kasus_check: TRUE if any condition flag is TRUE (NULLs treated as not-a-case)
    coalesce(ibu_kek_check, false)
        or coalesce(ibu_hipertensi_check, false)
        or coalesce(baduta_haz_check, false)
        or coalesce(baduta_wf_check, false)          as kasus_check,

    -- kasus: which condition(s), joined with ' + '. NULL when no case.
    -- All four flags are TRUE = problem. ibu_kek_check was reversed at staging to
    -- TRUE = KEK case (register: ibu_lila < 23.5; epus: negates its upstream flag).
    nullif(
        concat_ws(
            ' + ',
            case when ibu_kek_check        then 'KEK' end,
            case when ibu_hipertensi_check then 'Hipertensi' end,
            case when baduta_haz_check     then 'Stunting' end,
            case when baduta_wf_check      then 'Weight Faltering' end
        ),
        ''
    )                                                as kasus

from unioned
