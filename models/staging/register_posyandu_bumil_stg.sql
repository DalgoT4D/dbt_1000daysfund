{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["register_posyandu_bumil_stg", "staging"]
) }}

with source as (

    select * from {{ source('raw_sheets', 'register_posyandu_bumil') }}

),

typed as (

    select
        -- visit
        nullif(btrim(kunjungan_tanggal), '')::date              as kunjungan_tanggal,

        -- cadre (CHW) & health worker (HW) names
        nullif(btrim(chw_nama_1), '')                           as chw_nama_1,
        nullif(btrim(chw_nama_2), '')                           as chw_nama_2,
        nullif(btrim(chw_nama_3), '')                           as chw_nama_3,
        nullif(btrim(chw_nama_4), '')                           as chw_nama_4,
        nullif(btrim(chw_nama_5), '')                           as chw_nama_5,
        nullif(btrim(chw_nama_6), '')                           as chw_nama_6,
        nullif(btrim(hw_nama_1), '')                            as hw_nama_1,
        nullif(btrim(hw_nama_2), '')                            as hw_nama_2,
        nullif(btrim(hw_nama_3), '')                            as hw_nama_3,
        nullif(btrim(hw_nama_4), '')                            as hw_nama_4,
        nullif(btrim(hw_nama_5), '')                            as hw_nama_5,

        -- geography / facility
        nullif(btrim(provinsi), '')                             as provinsi,
        nullif(btrim(kota_kabupaten), '')                       as kota_kabupaten,
        nullif(btrim(kecamatan), '')                            as kecamatan,
        nullif(btrim(desa_kelurahan), '')                       as desa_kelurahan,
        nullif(btrim(puskesmas), '')                            as puskesmas,
        nullif(btrim(posyandu), '')                             as posyandu,

        -- mother identity & measures
        nullif(btrim(ibu_nama), '')                             as ibu_nama,
        nullif(btrim(ibu_usia), '')::int                        as ibu_usia,
        nullif(btrim(ibu_hpht), '')::date                       as ibu_hpht,
        nullif(btrim(ibu_tekanan_darah_sistolik), '')::numeric
                                                                as ibu_tekanan_darah_sistolik,
        nullif(btrim(ibu_tekanan_darah_diastolik), '')::numeric as ibu_tekanan_darah_diastolik,
        nullif(btrim(ibu_lila), '')::numeric                    as ibu_lila,
        nullif(btrim(ibu_ttd_mulai), '')::numeric               as ibu_ttd_mulai,
        nullif(btrim(ibu_ttd_jumlah_raw), '')::numeric          as ibu_ttd_jumlah_raw,
        nullif(btrim(ibu_anc_jumlah), '')::int                  as ibu_anc_jumlah,

        -- home-visit count (null = no home visit yet, not missing)
        nullif(btrim(kr_jumlah), '')::int                       as kr_jumlah,
        nullif(btrim(catatan), '')                              as catatan

    from source

),

gestational_age as (

    select
        *,
        (kunjungan_tanggal - ibu_hpht) / 7 as ibu_usia_kehamilan_minggu,  -- completed weeks
        (kunjungan_tanggal - ibu_hpht)     as ibu_usia_kehamilan_hari,    -- total days

        case
            when (kunjungan_tanggal - ibu_hpht) is null   then null
            when (kunjungan_tanggal - ibu_hpht) / 7 < 14  then 1
            when (kunjungan_tanggal - ibu_hpht) / 7 < 28  then 2
            else 3
        end as ibu_trimester

    from typed

),

ifa as (

    select
        *,
        -- tablets taken / days eligible for IFA (GA days - start week converted to days)
        round(
            ibu_ttd_jumlah_raw
            / nullif(ibu_usia_kehamilan_hari - (ibu_ttd_mulai * 7), 0)::numeric,
            2
        ) as ibu_ttd_perc

    from gestational_age

),

checks as (

    select
        *,

        -- KEK screening: LILA < 23.5 cm  (TRUE = KEK case / undernourished)
        (ibu_lila < 23.5) as ibu_kek_check,

        -- minimum ANC for gestational age
        case
            when ibu_usia_kehamilan_minggu is null or ibu_anc_jumlah is null then null
            when ibu_usia_kehamilan_minggu >= 36 and ibu_anc_jumlah >= 6 then true
            when ibu_usia_kehamilan_minggu >= 32 and ibu_anc_jumlah >= 5 then true
            when ibu_usia_kehamilan_minggu >= 28 and ibu_anc_jumlah >= 4 then true
            when ibu_usia_kehamilan_minggu >= 24 and ibu_anc_jumlah >= 3 then true
            when ibu_usia_kehamilan_minggu >= 20 and ibu_anc_jumlah >= 2 then true
            when ibu_usia_kehamilan_minggu <  20 and ibu_anc_jumlah >= 1 then true
            else false
        end as ibu_anc_check,

        -- hypertension: systolic >= 140 OR diastolic >= 90
        (ibu_tekanan_darah_sistolik >= 140)
            or (ibu_tekanan_darah_diastolik >= 90) as ibu_hipertensi_check,

        -- high-risk maternal age: < 17 or > 35
        (ibu_usia < 17 or ibu_usia > 35) as ibu_risti_check,

        -- IFA adherence: >= 100% of eligible days covered
        (ibu_ttd_perc >= 1) as ibu_ttd_check

    from ifa

),

final as (

    select
        -- visit
        kunjungan_tanggal,
        extract(year from kunjungan_tanggal)::int as year,
        extract(year from kunjungan_tanggal)::int || '-Q' || extract(quarter from kunjungan_tanggal)::int as quarter,

        -- cadre / health worker
        chw_nama_1,
        chw_nama_2,
        chw_nama_3,
        chw_nama_4,
        chw_nama_5,
        chw_nama_6,
        hw_nama_1,
        hw_nama_2,
        hw_nama_3,
        hw_nama_4,
        hw_nama_5,

        -- geography / facility
        provinsi,
        kota_kabupaten,
        kecamatan,
        desa_kelurahan,
        puskesmas,
        posyandu,

        -- mother identity & measures
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

        -- derived: gestational age
        ibu_usia_kehamilan_minggu,
        ibu_usia_kehamilan_hari,
        ibu_trimester,

        -- derived: IFA
        ibu_ttd_perc,

        -- checks
        ibu_kek_check,
        ibu_anc_check,
        ibu_hipertensi_check,
        ibu_risti_check,
        ibu_ttd_check

    from checks

)

select * from final