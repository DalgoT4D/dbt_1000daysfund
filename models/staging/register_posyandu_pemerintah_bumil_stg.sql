{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["register_posyandu", "staging", "parent", "pemerintah"]
) }}

-- Government-sourced posyandu bumil register.
-- Mirrors register_posyandu_bumil_stg. Not collected by the government register:
--   chw_nama_1..6, hw_nama_1..5, ibu_ttd_mulai, ibu_ttd_jumlah_raw,
--   ibu_anc_jumlah, kr_jumlah
-- so ibu_ttd_perc, ibu_ttd_check and ibu_anc_check are emitted as typed nulls.
--
-- Coverage warning: in the first load, ibu_lila is present on ~36% of rows,
-- blood pressure on <2%, and posyandu on ~54%. ibu_kek_check and
-- ibu_hipertensi_check will therefore be mostly null - treat them as
-- "not measured", not "passed".

with source as (

    select * from {{ source('raw_sheets', 'register_posyandu_pemerintah_bumil') }}

),

typed as (

    select
        -- visit
        nullif(btrim(kunjungan_tanggal), '')::date              as kunjungan_tanggal,

        -- geography / facility
        nullif(btrim(provinsi), '')                             as provinsi,
        nullif(btrim(kota_kabupaten), '')                       as kota_kabupaten,
        nullif(btrim(kecamatan), '')                            as kecamatan,
        nullif(btrim(desa_kelurahan), '')                       as desa_kelurahan,
        nullif(btrim(puskesmas), '')                            as puskesmas,
        nullif(btrim(posyandu), '')                             as posyandu,

        -- mother identity & measures
        -- guarded casts: the source carries free-text like '0 T' / '9 T' in ibu_usia
        nullif(btrim(ibu_nama), '')                             as ibu_nama,
        case when btrim(coalesce(ibu_usia, '')) ~ '^\d+$'
             then btrim(ibu_usia)::int end                      as ibu_usia,
        case when btrim(coalesce(ibu_hpht, '')) ~ '^\d{4}-\d{2}-\d{2}$'
             then btrim(ibu_hpht)::date end                     as ibu_hpht,
        case when replace(btrim(coalesce(ibu_tekanan_darah_sistolik, '')), ',', '.') ~ '^\d+(\.\d+)?$'
             then replace(btrim(ibu_tekanan_darah_sistolik), ',', '.')::numeric end
                                                                as ibu_tekanan_darah_sistolik,
        case when replace(btrim(coalesce(ibu_tekanan_darah_diastolik, '')), ',', '.') ~ '^\d+(\.\d+)?$'
             then replace(btrim(ibu_tekanan_darah_diastolik), ',', '.')::numeric end
                                                                as ibu_tekanan_darah_diastolik,
        case when replace(btrim(coalesce(ibu_lila, '')), ',', '.') ~ '^\d+(\.\d+)?$'
             then replace(btrim(ibu_lila), ',', '.')::numeric end
                                                                as ibu_lila,

        nullif(btrim(catatan), '')                              as catatan,

        -- provenance from the ingestion layer
        nullif(btrim(source_tab), '')                           as source_tab,
        case when btrim(coalesce(source_row, '')) ~ '^\d+$'
             then btrim(source_row)::int end                    as source_row,
        nullif(btrim(loaded_at), '')::timestamp                 as loaded_at

    from source

),

keyed as (

    select
        *,
        row_number() over (
            partition by source_tab, posyandu, ibu_nama, kunjungan_tanggal
            order by source_row
        ) as dup_rank

    from typed

),

gestational_age as (

    -- HPHT is only trusted when it puts the visit inside 0-45 completed weeks.
    -- The first load has 26 rows outside that window (up to 526 weeks - year typos).
    select
        *,
        case when (kunjungan_tanggal - ibu_hpht) between 0 and 315
             then (kunjungan_tanggal - ibu_hpht) / 7 end as ibu_usia_kehamilan_minggu,  -- completed weeks
        case when (kunjungan_tanggal - ibu_hpht) between 0 and 315
             then (kunjungan_tanggal - ibu_hpht) end     as ibu_usia_kehamilan_hari,    -- total days

        case
            when (kunjungan_tanggal - ibu_hpht) not between 0 and 315 then null
            when (kunjungan_tanggal - ibu_hpht) / 7 < 14  then 1
            when (kunjungan_tanggal - ibu_hpht) / 7 < 28  then 2
            else 3
        end as ibu_trimester

    from keyed

),

checks as (

    select
        *,

        -- KEK screening: LILA < 23.5 cm  (TRUE = KEK)
        (ibu_lila < 23.5) as ibu_kek_check,

        -- hypertension: systolic >= 140 OR diastolic >= 90
        (ibu_tekanan_darah_sistolik >= 140)
            or (ibu_tekanan_darah_diastolik >= 90) as ibu_hipertensi_check,

        -- high-risk maternal age: < 17 or > 35
        (ibu_usia < 17 or ibu_usia > 35) as ibu_risti_check

    from gestational_age

),

final as (

    select
        -- visit
        kunjungan_tanggal,

        -- not collected in the government register
        null::text as chw_nama_1,
        null::text as chw_nama_2,
        null::text as chw_nama_3,
        null::text as chw_nama_4,
        null::text as chw_nama_5,
        null::text as chw_nama_6,
        null::text as hw_nama_1,
        null::text as hw_nama_2,
        null::text as hw_nama_3,
        null::text as hw_nama_4,
        null::text as hw_nama_5,

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
        null::numeric as ibu_ttd_mulai,
        null::numeric as ibu_ttd_jumlah_raw,
        null::int     as ibu_anc_jumlah,
        null::int     as kr_jumlah,
        catatan,

        -- derived: gestational age
        ibu_usia_kehamilan_minggu,
        ibu_usia_kehamilan_hari,
        ibu_trimester,

        -- derived: IFA (not computable - no TTD fields in this source)
        null::numeric as ibu_ttd_perc,

        -- checks
        ibu_kek_check,
        null::boolean as ibu_anc_check,   -- needs ibu_anc_jumlah
        ibu_hipertensi_check,
        ibu_risti_check,
        null::boolean as ibu_ttd_check,   -- needs ibu_ttd_mulai / ibu_ttd_jumlah_raw

        -- provenance
        source_tab,
        source_row,
        loaded_at,
        dup_rank,
        (dup_rank > 1) as is_duplicate

    from checks

)

select * from final