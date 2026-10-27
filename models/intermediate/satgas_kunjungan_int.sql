-- Model: Stacks Satgas maternal (bumil) and child (baduta) home-visit records into one schema.
-- Grain: one row per Satgas visit record (one sheet row), as staged.
-- Each row also gets src_person_id / unified_name from within-source person resolution
-- (macros/satgas_person_resolution.sql), so spelling changes of the same person share one id.
-- Each branch fills its own category's columns and typed-NULLs the other's.
--   * kunjungan_bulan_raw keeps the sheet's own month text; kunjungan_bulan is the first day
--     of the month derived from kunjungan_tanggal (NULL when the date did not parse).
--   * catatan: baduta has one notes column, bumil has two; bumil's are joined with ' | '.
--   * satgas_id is a row key built from category + source file + source row.
--   * Meta columns (source_file, source_row, satgas_id, src_person_id, unified_name,
--     unified_name_key) are the right-most columns.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=['satgas', 'intermediate']
) }}

with recursive

bumil as (

    select
        'bumil'::text                                         as responden_kategori,

        -- origin and geography
        kota_kabupaten,
        puskesmas,
        desa_kelurahan,
        posyandu,

        -- visit
        kunjungan_bulan                                       as kunjungan_bulan_raw,
        date_trunc('month', kunjungan_tanggal)::date          as kunjungan_bulan,
        kunjungan_tanggal,
        extract(year from kunjungan_tanggal)::int             as year,
        extract(year from kunjungan_tanggal)::int || '-Q'
            || extract(quarter from kunjungan_tanggal)::int   as quarter,
        kunjungan_ke,
        cast(null as int)                                     as kunjungan_jumlah,

        -- case
        kasus_raw,
        kasus_status,
        masalah_utama,

        -- people
        chw_nama_raw,
        satgas_nama_raw,

        -- mother
        ibu_nama,
        ibu_suami_nama,
        ibu_usia_kehamilan_raw,
        ibu_berat_badan,
        ibu_lila,
        ibu_tekanan_darah_raw,
        ibu_tekanan_darah_sistolik,
        ibu_tekanan_darah_diastolik,
        ibu_tekanan_darah_flag,

        -- child
        cast(null as text)                                    as baduta_nama,
        cast(null as text)                                    as baduta_pengasuh_nama,
        cast(null as text)                                    as baduta_usia_raw,
        cast(null as numeric)                                 as baduta_berat_badan,
        cast(null as numeric)                                 as baduta_tinggi_badan,

        -- visit content
        penyuluhan,
        cast(null as text)                                    as progres,
        nullif(concat_ws(' | ', catatan_1, catatan_2), '')    as catatan,

        -- meta: provenance and row key
        source_file,
        source_row,
        md5(concat_ws('|', 'bumil', source_file, source_row::text)) as satgas_id

    from {{ ref('satgas_bumil_stg') }}

),

baduta as (

    select
        'baduta'::text                                        as responden_kategori,

        -- origin and geography
        kota_kabupaten,
        puskesmas,
        desa_kelurahan,
        cast(null as text)                                    as posyandu,

        -- visit
        kunjungan_bulan                                       as kunjungan_bulan_raw,
        date_trunc('month', kunjungan_tanggal)::date          as kunjungan_bulan,
        kunjungan_tanggal,
        extract(year from kunjungan_tanggal)::int             as year,
        extract(year from kunjungan_tanggal)::int || '-Q'
            || extract(quarter from kunjungan_tanggal)::int   as quarter,
        kunjungan_ke,
        kunjungan_jumlah,

        -- case
        kasus_raw,
        kasus_status,
        masalah_utama,

        -- people
        chw_nama_raw,
        satgas_nama_raw,

        -- mother
        cast(null as text)                                    as ibu_nama,
        cast(null as text)                                    as ibu_suami_nama,
        cast(null as text)                                    as ibu_usia_kehamilan_raw,
        cast(null as numeric)                                 as ibu_berat_badan,
        cast(null as numeric)                                 as ibu_lila,
        cast(null as text)                                    as ibu_tekanan_darah_raw,
        cast(null as int)                                     as ibu_tekanan_darah_sistolik,
        cast(null as int)                                     as ibu_tekanan_darah_diastolik,
        cast(null as text)                                    as ibu_tekanan_darah_flag,

        -- child
        baduta_nama,
        baduta_pengasuh_nama,
        baduta_usia_raw,
        baduta_berat_badan,
        baduta_tinggi_badan,

        -- visit content
        penyuluhan,
        progres,
        catatan,

        -- meta: provenance and row key
        source_file,
        source_row,
        md5(concat_ws('|', 'baduta', source_file, source_row::text)) as satgas_id

    from {{ ref('satgas_baduta_stg') }}

),

-- Stack both categories.
unioned as (

    select * from bumil
    union all
    select * from baduta

),

-- Within-source person resolution.
{{ satgas_person_ctes('unioned', 'satgas') }}

select
    u.*,
    m.src_person_id,
    m.unified_name,
    {{ profile_name_key('m.unified_name') }} as unified_name_key
from unioned u
left join sp_person_map m
    on m.sp_variant_id = {{ satgas_variant_id('u') }}