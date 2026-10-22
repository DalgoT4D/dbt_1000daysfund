-- Model: Cleans maternal Posyandu records and derives pregnancy indicators.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["register_posyandu", "staging", "parent"]
) }}

-- Load raw maternal register records.
with source as (

    select * from {{ source('raw_sheets', 'register_posyandu_tdf_bumil') }}

),

-- Cast raw maternal register fields to analysis-ready types.
-- Numeric casts are regex-guarded so a malformed cell becomes null instead of failing the build.
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
        case when btrim(ibu_usia) ~ '^\d+$'
             then btrim(ibu_usia)::int end                      as ibu_usia,
        nullif(btrim(ibu_hpht), '')::date                       as ibu_hpht,
        case when replace(btrim(ibu_tekanan_darah_sistolik), ',', '.') ~ '^(\d+(\.\d*)?|\.\d+)$'
             then replace(btrim(ibu_tekanan_darah_sistolik), ',', '.')::numeric
        end                                                     as ibu_tekanan_darah_sistolik,
        case when replace(btrim(ibu_tekanan_darah_diastolik), ',', '.') ~ '^(\d+(\.\d*)?|\.\d+)$'
             then replace(btrim(ibu_tekanan_darah_diastolik), ',', '.')::numeric
        end                                                     as ibu_tekanan_darah_diastolik,
        case when replace(btrim(ibu_lila), ',', '.') ~ '^(\d+(\.\d*)?|\.\d+)$'
             then replace(btrim(ibu_lila), ',', '.')::numeric
        end                                                     as ibu_lila,
        case when replace(btrim(ibu_ttd_mulai), ',', '.') ~ '^(\d+(\.\d*)?|\.\d+)$'
             then replace(btrim(ibu_ttd_mulai), ',', '.')::numeric
        end                                                     as ibu_ttd_mulai,
        case when replace(btrim(ibu_ttd_jumlah_raw), ',', '.') ~ '^(\d+(\.\d*)?|\.\d+)$'
             then replace(btrim(ibu_ttd_jumlah_raw), ',', '.')::numeric
        end                                                     as ibu_ttd_jumlah_raw,
        case when btrim(ibu_anc_jumlah) ~ '^\d+$'
             then btrim(ibu_anc_jumlah)::int end                as ibu_anc_jumlah,

        -- home-visit count (null = no home visit yet, not missing)
        case when btrim(kr_jumlah) ~ '^\d+$'
             then btrim(kr_jumlah)::int end                     as kr_jumlah,
        nullif(btrim(catatan), '')                              as catatan,

        -- provenance from the ingestion layer
        case
            when upper(btrim(coalesce(is_verified, ''))) in ('TRUE',  'YA',    '1', 'Y') then true
            when upper(btrim(coalesce(is_verified, ''))) in ('FALSE', 'TIDAK', '0', 'N') then false
        end as is_verified,
        nullif(btrim(source_tab), '')                           as source_tab,
        case when btrim(coalesce(source_row, '')) ~ '^\d+$'
             then btrim(source_row)::int end                    as source_row,
        nullif(btrim(loaded_at), '')::timestamp                 as loaded_at

    from source

),

-- Rank duplicate records. Rows missing a name or visit date can't be matched
-- reliably (nulls would all fall into one partition), so they are kept as rank 1.
keyed as (

    select
        *,
        case
            when ibu_nama is null or kunjungan_tanggal is null then 1
            else row_number() over (
                partition by source_tab, posyandu, ibu_nama, kunjungan_tanggal
                order by source_row nulls last, loaded_at
            )
        end as dup_rank

    from typed

),

-- Gestational age in days. Null when either date is missing or HPHT is after the visit.
ga_days as (

    select
        *,
        case
            when kunjungan_tanggal is null or ibu_hpht is null then null
            when kunjungan_tanggal < ibu_hpht                  then null
            else kunjungan_tanggal - ibu_hpht
        end as ibu_usia_kehamilan_hari

    from keyed

),

-- Derive gestational weeks and trimester.
gestational_age as (

    select
        *,
        ibu_usia_kehamilan_hari / 7 as ibu_usia_kehamilan_minggu,  -- completed weeks

        case
            when ibu_usia_kehamilan_hari is null      then null
            when ibu_usia_kehamilan_hari / 7 < 14     then 1
            when ibu_usia_kehamilan_hari / 7 < 28     then 2
            else 3
        end as ibu_trimester

    from ga_days

),

-- Calculate iron-folic-acid adherence.
-- tablets taken / days eligible (GA days - start week in days).
-- Null when any input is missing or eligible days <= 0 (IFA start at/after the visit).
ifa as (

    select
        *,
        case
            when ibu_ttd_jumlah_raw is null
              or ibu_ttd_mulai is null
              or ibu_usia_kehamilan_hari is null               then null
            when ibu_usia_kehamilan_hari - (ibu_ttd_mulai * 7) <= 0 then null
            else round(
                ibu_ttd_jumlah_raw
                / (ibu_usia_kehamilan_hari - (ibu_ttd_mulai * 7))::numeric,
                2
            )
        end as ibu_ttd_perc

    from gestational_age

),

-- Derive maternal health and care checks. Missing inputs return null (unknown).
checks as (

    select
        *,

        -- KEK screening: LILA >= 23.5 cm  (TRUE = adequate, i.e. NOT KEK)
        case
            when ibu_lila is null then null
            else ibu_lila >= 23.5
        end as ibu_kek_check,

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
        -- one high reading is enough for TRUE; FALSE needs both readings present.
        case
            when ibu_tekanan_darah_sistolik >= 140
              or ibu_tekanan_darah_diastolik >= 90             then true
            when ibu_tekanan_darah_sistolik is null
              or ibu_tekanan_darah_diastolik is null           then null
            else false
        end as ibu_hipertensi_check,

        -- high-risk maternal age: < 17 or > 35
        case
            when ibu_usia is null then null
            else ibu_usia < 17 or ibu_usia > 35
        end as ibu_risti_check,

        -- IFA adherence: >= 100% of eligible days covered
        case
            when ibu_ttd_perc is null then null
            else ibu_ttd_perc >= 1
        end as ibu_ttd_check

    from ifa

),

-- Select the standardized maternal output columns.
final as (

    select
        -- visit
        extract(year from kunjungan_tanggal)::int as year,
        kunjungan_tanggal,

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
        ibu_ttd_check,

        -- provenance
        is_verified,
        source_tab,
        source_row,
        loaded_at,
        dup_rank,
        (dup_rank > 1) as is_duplicate
    from checks

)

select * from final