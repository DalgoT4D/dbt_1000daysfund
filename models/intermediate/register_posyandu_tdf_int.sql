-- Model: Stacks TDF maternal and child Posyandu records and resolves persons within the source.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["register_posyandu_tdf", "intermediate", "register_posyandu", "parent"]
) }}

-- TDF Posyandu register: bumil and baduta stacked into one schema.
-- Each branch fills its own category's columns and typed-NULLs the other's.
-- year/quarter are derived from kunjungan_tanggal. kasus_check / kasus are derived
-- from the four condition flags. Each row also gets src_person_id / unified_name from
-- within-source person resolution; src_person_id is only unique WITHIN this source.
-- baduta_wf_check (weight faltering) is derived here from consecutive monthly weights
-- against the KBM table (reference.kbm); it needs src_person_id, so it
-- cannot live in staging.

with recursive

-- Align self-collected maternal records to the shared schema.
reg_bumil as (

    select
        'bumil'                                  as responden_kategori,
        'register_posyandu'                      as source,
        kunjungan_tanggal                        as kunjungan_tanggal,
        extract(year from kunjungan_tanggal)::int as year,
        extract(year from kunjungan_tanggal)::int || '-Q' || extract(quarter from kunjungan_tanggal)::int as quarter,
        chw_nama_1                               as chw_nama_1,
        chw_nama_2                               as chw_nama_2,
        chw_nama_3                               as chw_nama_3,
        chw_nama_4                               as chw_nama_4,
        chw_nama_5                               as chw_nama_5,
        chw_nama_6                               as chw_nama_6,
        hw_nama_1                                as hw_nama_1,
        hw_nama_2                                as hw_nama_2,
        hw_nama_3                                as hw_nama_3,
        hw_nama_4                                as hw_nama_4,
        hw_nama_5                                as hw_nama_5,
        provinsi                                 as provinsi,
        kota_kabupaten                           as kota_kabupaten,
        kecamatan                                as kecamatan,
        desa_kelurahan                           as desa_kelurahan,
        puskesmas                                as puskesmas,
        posyandu                                 as posyandu,
        kr_jumlah                                as kr_jumlah,
        catatan                                  as catatan,
        ibu_nama                                 as ibu_nama,
        ibu_usia                                 as ibu_usia,
        ibu_hpht                                 as ibu_hpht,
        ibu_tekanan_darah_sistolik               as ibu_tekanan_darah_sistolik,
        ibu_tekanan_darah_diastolik              as ibu_tekanan_darah_diastolik,
        ibu_lila                                 as ibu_lila,
        ibu_ttd_mulai                            as ibu_ttd_mulai,
        ibu_ttd_jumlah_raw                       as ibu_ttd_jumlah_raw,
        ibu_anc_jumlah                           as ibu_anc_jumlah,
        ibu_usia_kehamilan_minggu                as ibu_usia_kehamilan_minggu,
        ibu_usia_kehamilan_hari                  as ibu_usia_kehamilan_hari,
        ibu_trimester                            as ibu_trimester,
        ibu_ttd_perc                             as ibu_ttd_perc,
        ibu_kek_check                            as ibu_kek_check,
        ibu_anc_check                            as ibu_anc_check,
        ibu_hipertensi_check                     as ibu_hipertensi_check,
        ibu_risti_check                          as ibu_risti_check,
        ibu_ttd_check                            as ibu_ttd_check,
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
        cast(null as boolean ) as baduta_protein_check
    from {{ ref('register_posyandu_tdf_bumil_stg') }}

),

-- Align self-collected child records to the shared schema.
reg_baduta as (

    select
        'baduta'                                 as responden_kategori,
        'register_posyandu'                      as source,
        kunjungan_tanggal                        as kunjungan_tanggal,
        extract(year from kunjungan_tanggal)::int as year,
        extract(year from kunjungan_tanggal)::int || '-Q' || extract(quarter from kunjungan_tanggal)::int as quarter,
        chw_nama_1                               as chw_nama_1,
        chw_nama_2                               as chw_nama_2,
        chw_nama_3                               as chw_nama_3,
        chw_nama_4                               as chw_nama_4,
        chw_nama_5                               as chw_nama_5,
        chw_nama_6                               as chw_nama_6,
        hw_nama_1                                as hw_nama_1,
        hw_nama_2                                as hw_nama_2,
        hw_nama_3                                as hw_nama_3,
        hw_nama_4                                as hw_nama_4,
        hw_nama_5                                as hw_nama_5,
        provinsi                                 as provinsi,
        kota_kabupaten                           as kota_kabupaten,
        kecamatan                                as kecamatan,
        desa_kelurahan                           as desa_kelurahan,
        puskesmas                                as puskesmas,
        posyandu                                 as posyandu,
        kr_jumlah                                as kr_jumlah,
        catatan                                  as catatan,
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
        baduta_nama                              as baduta_nama,
        baduta_gender                            as baduta_gender,
        baduta_tanggal_lahir                     as baduta_tanggal_lahir,
        baduta_usia_bulan                        as baduta_usia_bulan,
        baduta_pengasuh_nama                     as baduta_pengasuh_nama,
        baduta_asi_biner                         as baduta_asi_biner,
        baduta_mpasi_biner                       as baduta_mpasi_biner,
        baduta_protein_biner                     as baduta_protein_biner,
        baduta_berat_badan                       as baduta_berat_badan,
        baduta_tinggi_badan                      as baduta_tinggi_badan,
        baduta_waz                               as baduta_waz,
        baduta_haz                               as baduta_haz,
        baduta_waz_check                         as baduta_waz_check,
        baduta_haz_check                         as baduta_haz_check,
        baduta_asi_check                         as baduta_asi_check,
        baduta_protein_check                     as baduta_protein_check
    from {{ ref('register_posyandu_tdf_baduta_stg') }}

),

-- Stack all aligned Posyandu records.
unioned as (

    select * from reg_bumil
    union all
    select * from reg_baduta
),

-- Within-source person resolution (see macros/register_person_resolution.sql).
{{ register_person_ctes('unioned', 'tdf') }},

-- Attach the within-source person id to every row.
with_person as (

    select
        u.*,
        m.src_person_id,
        m.unified_name,
        {{ profile_name_key('m.unified_name') }} as unified_name_key
    from unioned u
    left join rp_person_map m
        on m.rp_variant_id = {{ register_variant_id('u') }}

),

-- Weight faltering. One weight per baduta per month (latest weighing in the month),
-- compared with the previous month's weight. Only back-to-back months are judged:
-- a gap of 2+ months, a first weighing, or an age without a KBM gives NULL.
wf_monthly as (

    select distinct on (src_person_id, date_trunc('month', kunjungan_tanggal))
        src_person_id,
        date_trunc('month', kunjungan_tanggal)::date as visit_month,
        baduta_berat_badan,
        baduta_usia_bulan
    from with_person
    where responden_kategori = 'baduta'
      and src_person_id is not null
      and kunjungan_tanggal is not null
      and baduta_berat_badan is not null
    order by src_person_id, date_trunc('month', kunjungan_tanggal), kunjungan_tanggal desc

),

wf_gain as (

    select
        *,
        lag(visit_month)        over w as prev_visit_month,
        lag(baduta_berat_badan) over w as prev_berat_badan
    from wf_monthly
    window w as (partition by src_person_id order by visit_month)

),

wf as (

    select
        g.src_person_id,
        g.visit_month,
        case when g.prev_visit_month = (g.visit_month - interval '1 month')::date
             then round((g.baduta_berat_badan - g.prev_berat_badan) * 1000)
        end                                                    as baduta_bb_naik_gram,
        k.kbm_gram                                             as baduta_kbm_gram,
        case
            when g.prev_visit_month is distinct from (g.visit_month - interval '1 month')::date then null
            when k.kbm_gram is null                                                          then null
            else (g.baduta_berat_badan - g.prev_berat_badan) * 1000 < k.kbm_gram
        end                                                    as baduta_wf_check
    from wf_gain g
    left join (
        select
            usia_bulan_min::numeric as usia_bulan_min,
            usia_bulan_max::numeric as usia_bulan_max,
            kbm_gram::numeric       as kbm_gram
        from {{ source('reference', 'kbm') }}
    ) k
        on g.baduta_usia_bulan between k.usia_bulan_min and k.usia_bulan_max

),

-- Rows + weight faltering.
with_wf as (

    select
        p.*,
        wf.baduta_bb_naik_gram,
        wf.baduta_kbm_gram,
        wf.baduta_wf_check
    from with_person p
    left join wf
        on  wf.src_person_id = p.src_person_id
        and wf.visit_month   = date_trunc('month', p.kunjungan_tanggal)::date
        and p.responden_kategori = 'baduta'

)

select
    *,

    -- kasus_check: TRUE if any condition flag is TRUE (NULLs treated as not-a-case)
    coalesce(ibu_kek_check, false)
        or coalesce(ibu_hipertensi_check, false)
        or coalesce(baduta_haz_check, false)
        or coalesce(baduta_wf_check, false)          as kasus_check,

    -- kasus: which condition(s), joined with ' + '. NULL when no case.
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

from with_wf