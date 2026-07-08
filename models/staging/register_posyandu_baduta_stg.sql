{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["register_posyandu_baduta_zscore", "marts"]
) }}

-- typed: normalize raw text columns into the data types the downstream logic
-- needs, reading directly from the raw Google Sheet source. This is where:
--   - visit date becomes DATE
--   - DOB becomes DATE, supporting both YYYY-MM-DD and MM/DD/YYYY inputs
--   - weight / height become NUMERIC
-- The z-scores rely on these typed fields, so any parsing issue here will
-- usually flow through as a NULL z-score later.
with typed as (

    select
        nullif(btrim(kunjungan_tanggal), '')::date              as kunjungan_tanggal,

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

        nullif(btrim(provinsi), '')                             as provinsi,
        nullif(btrim(kota_kabupaten), '')                       as kota_kabupaten,
        nullif(btrim(kecamatan), '')                            as kecamatan,
        nullif(btrim(desa_kelurahan), '')                       as desa_kelurahan,
        nullif(btrim(puskesmas), '')                            as puskesmas,
        nullif(btrim(posyandu), '')                             as posyandu,

        nullif(btrim(baduta_nama), '')                          as baduta_nama,
        nullif(btrim(baduta_gender), '')                        as baduta_gender,
        nullif(btrim(baduta_pengasuh_nama), '')                 as baduta_pengasuh_nama,
        nullif(btrim(baduta_asi_biner), '')                     as baduta_asi_biner,
        nullif(btrim(baduta_mpasi_biner), '')                   as baduta_mpasi_biner,
        nullif(btrim(baduta_protein_biner), '')                 as baduta_protein_biner,

        case
            when nullif(btrim(baduta_tanggal_lahir), '') is null then null
            when btrim(baduta_tanggal_lahir) ~ '^\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])$'
                then to_date(btrim(baduta_tanggal_lahir), 'YYYY-MM-DD')
            when btrim(baduta_tanggal_lahir) ~ '^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/\d{4}$'
                then to_date(btrim(baduta_tanggal_lahir), 'MM/DD/YYYY')
            else null
        end                                                     as baduta_tanggal_lahir,

        case
            when nullif(btrim(baduta_berat_badan), '') is null then null
            else replace(btrim(baduta_berat_badan), ',', '.')::numeric
        end                                                     as baduta_berat_badan,

        case
            when nullif(btrim(baduta_tinggi_badan), '') is null then null
            else replace(btrim(baduta_tinggi_badan), ',', '.')::numeric
        end                                                     as baduta_tinggi_badan,

        nullif(btrim(kr_jumlah), '')::int                       as kr_jumlah,
        nullif(btrim(catatan), '')                              as catatan

    from {{ source('raw_sheets', 'register_posyandu_baduta') }}

),

-- aged: derive the WHO lookup inputs from the cleaned row.
--   - baduta_usia_bulan is age in completed months at the visit date
--   - baduta_who_sex converts local gender labels to WHO reference codes
-- The WHO standard tables are keyed by (sex, month), so this CTE creates the
-- exact join keys used for z-score lookup.
aged as (

    select
        *,

        case
            when kunjungan_tanggal is null or baduta_tanggal_lahir is null then null
            else (extract(year from age(kunjungan_tanggal, baduta_tanggal_lahir))::int * 12)
               + extract(month from age(kunjungan_tanggal, baduta_tanggal_lahir))::int
        end as baduta_usia_bulan,

        case
            when lower(coalesce(baduta_gender, '')) in ('laki-laki', 'laki laki', 'male', 'm') then 'M'
            when lower(coalesce(baduta_gender, '')) in ('perempuan', 'female', 'f') then 'F'
            else null
        end as baduta_who_sex

    from typed

),

-- ref_wfa: WHO weight-for-age reference table.
-- For each sex + month combination, WHO provides LMS parameters:
--   L = Box-Cox power
--   M = median
--   S = coefficient of variation
-- These parameters are used in the LMS z-score formula for body weight.
ref_wfa as (

    select
        sex,
        month::int    as month,
        l::numeric    as l,
        m::numeric    as m,
        s::numeric    as s
    from {{ source('reference', 'who_wfa') }}

),

-- ref_lhfa: WHO length/height-for-age reference table.
-- Same LMS structure as WFA, but the parameters here are for length/height
-- relative to age.
ref_lhfa as (

    select
        sex,
        month::int    as month,
        l::numeric    as l,
        m::numeric    as m,
        s::numeric    as s
    from {{ source('reference', 'who_lhfa') }}

),

-- zscored: join each child row to the correct WHO reference row, then compute
-- the actual z-scores.
--
-- z_wfa uses:
--   observed weight + WFA LMS parameters
--
-- z_hfa uses:
--   observed height/length + LHFA LMS parameters
--
-- The macro `who_lms_zscore(...)` implements the WHO LMS formula:
--   z = ((y / M)^L - 1) / (S * L)
-- and also applies WHO's tail adjustment for values beyond +/- 3 SD.
--
-- In practical terms:
--   z = 0   means exactly at the WHO median
--   z < 0   means below the WHO median
--   z > 0   means above the WHO median
--   z <= -2 is commonly used as an undernutrition / stunting threshold,
--           depending on the indicator being interpreted
zscored as (

    select
        a.*,

        round(
            ({{ who_lms_zscore('a.baduta_berat_badan', 'w.l', 'w.m', 'w.s') }})::numeric,
            2
        ) as z_wfa,

        round(
            ({{ who_lms_zscore('a.baduta_tinggi_badan', 'h.l', 'h.m', 'h.s') }})::numeric,
            2
        ) as z_hfa

    from aged a

    left join ref_wfa w
        on  w.month = a.baduta_usia_bulan
        and w.sex   = a.baduta_who_sex

    left join ref_lhfa h
        on  h.month = a.baduta_usia_bulan
        and h.sex   = a.baduta_who_sex

)

-- terminal select: return the business-facing output directly from zscored.
-- We keep this explicit projection so helper fields like baduta_who_sex stay
-- internal, while the final column order remains stable.
select
    kunjungan_tanggal,

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

    provinsi,
    kota_kabupaten,
    kecamatan,
    desa_kelurahan,
    puskesmas,
    posyandu,

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

    z_wfa,
    z_hfa

from zscored
