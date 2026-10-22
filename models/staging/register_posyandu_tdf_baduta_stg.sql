-- Model: Cleans child Posyandu records and derives growth indicators.
{{ config(materialized='table', persist_docs={'relation': true, 'columns': true},
quoting={'identifier': true}, tags=["register_posyandu", "staging", "parent"]) }}

-- typed: cast the raw sheet fields into the types needed for age and z-score logic.
-- Numeric casts are regex-guarded so a malformed cell becomes null instead of failing the build.
with typed as (
    select
        nullif(btrim(kunjungan_tanggal), '')::date as kunjungan_tanggal, -- visit date
        nullif(btrim(chw_nama_1), '') as chw_nama_1, nullif(btrim(chw_nama_2), '') as chw_nama_2,
        nullif(btrim(chw_nama_3), '') as chw_nama_3, nullif(btrim(chw_nama_4), '') as chw_nama_4,
        nullif(btrim(chw_nama_5), '') as chw_nama_5, nullif(btrim(chw_nama_6), '') as chw_nama_6,
        nullif(btrim(hw_nama_1), '') as hw_nama_1, nullif(btrim(hw_nama_2), '') as hw_nama_2,
        nullif(btrim(hw_nama_3), '') as hw_nama_3, nullif(btrim(hw_nama_4), '') as hw_nama_4,
        nullif(btrim(hw_nama_5), '') as hw_nama_5,
        nullif(btrim(provinsi), '') as provinsi, nullif(btrim(kota_kabupaten), '') as kota_kabupaten,
        nullif(btrim(kecamatan), '') as kecamatan, nullif(btrim(desa_kelurahan), '') as desa_kelurahan,
        nullif(btrim(puskesmas), '') as puskesmas, nullif(btrim(posyandu), '') as posyandu,
        nullif(btrim(baduta_nama), '') as baduta_nama, nullif(btrim(baduta_gender), '') as baduta_gender,
        nullif(btrim(baduta_pengasuh_nama), '') as baduta_pengasuh_nama,
        nullif(btrim(baduta_asi_biner), '') as baduta_asi_biner,
        nullif(btrim(baduta_mpasi_biner), '') as baduta_mpasi_biner,
        nullif(btrim(baduta_protein_biner), '') as baduta_protein_biner,
        case
            when nullif(btrim(baduta_tanggal_lahir), '') is null then null
            when btrim(baduta_tanggal_lahir) ~ '^\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])$' then to_date(btrim(baduta_tanggal_lahir), 'YYYY-MM-DD')
            when btrim(baduta_tanggal_lahir) ~ '^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/\d{4}$' then to_date(btrim(baduta_tanggal_lahir), 'MM/DD/YYYY')
            else null
        end as baduta_tanggal_lahir,
        case
            when replace(btrim(baduta_berat_badan), ',', '.') ~ '^(\d+(\.\d*)?|\.\d+)$'
                then replace(btrim(baduta_berat_badan), ',', '.')::numeric
        end as baduta_berat_badan, -- weight
        case
            when replace(btrim(baduta_tinggi_badan), ',', '.') ~ '^(\d+(\.\d*)?|\.\d+)$'
                then replace(btrim(baduta_tinggi_badan), ',', '.')::numeric
        end as baduta_tinggi_badan, -- height
        case when btrim(kr_jumlah) ~ '^\d+$' then btrim(kr_jumlah)::int end as kr_jumlah, -- qty
        nullif(btrim(catatan), '') as catatan, -- notes

        -- provenance from the ingestion layer
        case
            when upper(btrim(coalesce(is_verified, ''))) in ('TRUE',  'YA',    '1', 'Y') then true
            when upper(btrim(coalesce(is_verified, ''))) in ('FALSE', 'TIDAK', '0', 'N') then false
        end as is_verified,
        nullif(btrim(source_tab), '') as source_tab,
        case when btrim(coalesce(source_row, '')) ~ '^\d+$' then btrim(source_row)::int end as source_row,
        nullif(btrim(loaded_at), '')::timestamp as loaded_at
    from {{ source('raw_sheets', 'register_posyandu_tdf_baduta') }}
),

-- Rank duplicate records. Rows missing a name or visit date can't be matched
-- reliably (nulls would all fall into one partition), so they are kept as rank 1.
keyed as (
    select
        *,
        case
            when baduta_nama is null or kunjungan_tanggal is null then 1
            else row_number() over (
                partition by source_tab, posyandu, baduta_nama, baduta_tanggal_lahir, kunjungan_tanggal
                order by source_row nulls last, loaded_at
            )
        end as dup_rank
    from typed
),

-- aged: derive the WHO lookup keys from the cleaned row.
-- Visit dates before the birth date give no age (would be negative).
aged as (
    select
        *,
        case
            when kunjungan_tanggal is null or baduta_tanggal_lahir is null then null
            when kunjungan_tanggal < baduta_tanggal_lahir then null
            else (extract(year from age(kunjungan_tanggal, baduta_tanggal_lahir))::int * 12)
                 + extract(month from age(kunjungan_tanggal, baduta_tanggal_lahir))::int
        end as baduta_usia_bulan,
        case
            when lower(coalesce(baduta_gender, '')) in ('laki-laki', 'laki laki', 'male', 'm') then 'M'
            when lower(coalesce(baduta_gender, '')) in ('perempuan', 'female', 'f') then 'F'
            else null
        end as baduta_who_sex
    from keyed
),

-- ref_wfa / ref_lhfa: WHO growth lookup tables keyed by sex + completed month of age.
ref_wfa as (
    select sex, month::int as month, l::numeric as l, m::numeric as m, s::numeric as s
    from {{ source('reference', 'who_wfa') }}
),

ref_lhfa as (
    select sex, month::int as month, m::numeric as m, s::numeric as s, l::numeric as l
    from {{ source('reference', 'who_lhfa') }}
),

-- lms_joined: bring the WHO LMS values onto each child record and flag whether
-- the reference row is usable (all of L, M, S present; M and S positive).
lms_joined as (
    select
        a.*,
        w.l as wfa_l,
        w.m as wfa_m,
        w.s as wfa_s,
        h.m as hfa_m,
        h.s as hfa_s,
        h.l as hfa_l,
        (w.l is not null and w.m > 0 and w.s > 0) as wfa_ref_ok,
        (h.l is not null and h.m > 0 and h.s > 0) as hfa_ref_ok
    from aged a
    left join ref_wfa w on w.month = a.baduta_usia_bulan and w.sex = a.baduta_who_sex
    left join ref_lhfa h on h.month = a.baduta_usia_bulan and h.sex = a.baduta_who_sex
),

-- zscore_inputs:
--   raw LMS z-score = ((y / M)^L - 1) / (S * L)      (L = 0: ln(y / M) / S)
--   WHO SD cutoff   = M * (1 + L * S * k)^(1 / L)    (L = 0: M * exp(S * k))
-- Guards: measurement must be > 0 (power of 0/negative errors in Postgres),
-- reference must be usable, and the cutoff base (1 + L*S*k) must be > 0.
zscore_inputs as (
    select
        l.*,

        -- weight-for-age
        case when wfa_ref_ok and baduta_berat_badan > 0 then
            case when wfa_l = 0 then ln(baduta_berat_badan / wfa_m) / wfa_s
                 else (power(baduta_berat_badan / wfa_m, wfa_l) - 1) / (wfa_s * wfa_l) end
        end as z_wfa_raw,
        case when wfa_ref_ok then
            case when wfa_l = 0 then wfa_m * exp(wfa_s * 3)
                 when 1 + wfa_l * wfa_s * 3 > 0 then wfa_m * power(1 + wfa_l * wfa_s * 3, 1.0 / wfa_l) end
        end as wfa_sd3pos,
        case when wfa_ref_ok then
            case when wfa_l = 0 then wfa_m * exp(wfa_s * 2)
                 when 1 + wfa_l * wfa_s * 2 > 0 then wfa_m * power(1 + wfa_l * wfa_s * 2, 1.0 / wfa_l) end
        end as wfa_sd2pos,
        case when wfa_ref_ok then
            case when wfa_l = 0 then wfa_m * exp(wfa_s * -2)
                 when 1 + wfa_l * wfa_s * -2 > 0 then wfa_m * power(1 + wfa_l * wfa_s * -2, 1.0 / wfa_l) end
        end as wfa_sd2neg,
        case when wfa_ref_ok then
            case when wfa_l = 0 then wfa_m * exp(wfa_s * -3)
                 when 1 + wfa_l * wfa_s * -3 > 0 then wfa_m * power(1 + wfa_l * wfa_s * -3, 1.0 / wfa_l) end
        end as wfa_sd3neg,

        -- length/height-for-age
        case when hfa_ref_ok and baduta_tinggi_badan > 0 then
            case when hfa_l = 0 then ln(baduta_tinggi_badan / hfa_m) / hfa_s
                 else (power(baduta_tinggi_badan / hfa_m, hfa_l) - 1) / (hfa_s * hfa_l) end
        end as z_hfa_raw,
        case when hfa_ref_ok then
            case when hfa_l = 0 then hfa_m * exp(hfa_s * 3)
                 when 1 + hfa_l * hfa_s * 3 > 0 then hfa_m * power(1 + hfa_l * hfa_s * 3, 1.0 / hfa_l) end
        end as hfa_sd3pos,
        case when hfa_ref_ok then
            case when hfa_l = 0 then hfa_m * exp(hfa_s * 2)
                 when 1 + hfa_l * hfa_s * 2 > 0 then hfa_m * power(1 + hfa_l * hfa_s * 2, 1.0 / hfa_l) end
        end as hfa_sd2pos,
        case when hfa_ref_ok then
            case when hfa_l = 0 then hfa_m * exp(hfa_s * -2)
                 when 1 + hfa_l * hfa_s * -2 > 0 then hfa_m * power(1 + hfa_l * hfa_s * -2, 1.0 / hfa_l) end
        end as hfa_sd2neg,
        case when hfa_ref_ok then
            case when hfa_l = 0 then hfa_m * exp(hfa_s * -3)
                 when 1 + hfa_l * hfa_s * -3 > 0 then hfa_m * power(1 + hfa_l * hfa_s * -3, 1.0 / hfa_l) end
        end as hfa_sd3neg
    from lms_joined l
),

-- zscored: keep the raw z-score inside +/-3 SD, otherwise switch to the WHO
-- linear tail adjustment using the +/-2 SD and +/-3 SD cutoffs.
-- Denominators are nullif-guarded; a null cutoff yields a null z-score.
-- https://cdn.who.int/media/docs/default-source/child-growth/growth-reference-5-19-years/computation.pdf?sfvrsn=c2ff6a95_4
zscored as (
    select
        z.*,
        round(case
            when z_wfa_raw > 3  then  3 + (baduta_berat_badan - wfa_sd3pos) / nullif(wfa_sd3pos - wfa_sd2pos, 0)
            when z_wfa_raw < -3 then -3 + (baduta_berat_badan - wfa_sd3neg) / nullif(wfa_sd2neg - wfa_sd3neg, 0)
            else z_wfa_raw
        end, 2) as z_wfa,
        round(case
            when z_hfa_raw > 3  then  3 + (baduta_tinggi_badan - hfa_sd3pos) / nullif(hfa_sd3pos - hfa_sd2pos, 0)
            when z_hfa_raw < -3 then -3 + (baduta_tinggi_badan - hfa_sd3neg) / nullif(hfa_sd2neg - hfa_sd3neg, 0)
            else z_hfa_raw
        end, 2) as z_hfa
    from zscore_inputs z
),

-- flagged: derive boolean z-score and age-based feeding flags for reporting.
-- Any missing input returns null (unknown) rather than false.
flagged as (
    select
        z.*,
        case
            when z_wfa is null then null
            else z_wfa <= -2
        end as baduta_waz_check,
        case
            when z_hfa is null then null
            else z_hfa <= -2
        end as baduta_haz_check,
        case
            when baduta_usia_bulan is null or baduta_usia_bulan >= 6 then null
            when baduta_asi_biner is null or baduta_mpasi_biner is null then null
            else upper(baduta_asi_biner) = 'TRUE' and upper(baduta_mpasi_biner) = 'FALSE'
        end as baduta_asi_check,
        case
            when baduta_usia_bulan is null or baduta_usia_bulan < 6 then null
            when baduta_protein_biner is null then null
            else upper(baduta_protein_biner) = 'TRUE'
        end as baduta_protein_check
    from zscored z
)

-- terminal select: expose the business-facing columns only, keeping helper
-- fields like baduta_who_sex and the raw LMS pieces internal.
select
    extract(year from kunjungan_tanggal)::int as year,
    kunjungan_tanggal,
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
    z_wfa as baduta_waz,
    z_hfa as baduta_haz,
    baduta_waz_check,
    baduta_haz_check,
    baduta_asi_check,
    baduta_protein_check,
    null::boolean as baduta_wf_check, -- if a child hasnt been weighed in 3 months
    -- provenance
    is_verified,
    source_tab,
    source_row,
    loaded_at,
    dup_rank,
    (dup_rank > 1) as is_duplicate
from flagged