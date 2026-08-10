{{ config(materialized='table', persist_docs={'relation': true, 'columns': true},
quoting={'identifier': true}, tags=["register_posyandu", "staging", "parent", "pemerintah"]) }}

-- Government-sourced posyandu baduta register.
-- Mirrors register_posyandu_baduta_stg. Columns NOT collected by the government
-- register are emitted as typed nulls so this model unions cleanly with the
-- self-collected model:
--   chw_nama_1..6, hw_nama_1..5, baduta_asi_biner, baduta_mpasi_biner,
--   baduta_protein_biner, kr_jumlah  -> and therefore asi_check / protein_check.

-- typed: cast the raw sheet fields into the types needed for age and z-score logic.
with typed as (
    select
        nullif(btrim(kunjungan_tanggal), '')::date as kunjungan_tanggal, -- visit date
        nullif(btrim(provinsi), '') as provinsi, nullif(btrim(kota_kabupaten), '') as kota_kabupaten,
        nullif(btrim(kecamatan), '') as kecamatan, nullif(btrim(desa_kelurahan), '') as desa_kelurahan,
        nullif(btrim(puskesmas), '') as puskesmas, nullif(btrim(posyandu), '') as posyandu,
        nullif(btrim(baduta_nama), '') as baduta_nama, nullif(btrim(baduta_gender), '') as baduta_gender,
        nullif(btrim(baduta_pengasuh_nama), '') as baduta_pengasuh_nama,
        case
            when nullif(btrim(baduta_tanggal_lahir), '') is null then null
            when btrim(baduta_tanggal_lahir) ~ '^\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])$' then to_date(btrim(baduta_tanggal_lahir), 'YYYY-MM-DD')
            when btrim(baduta_tanggal_lahir) ~ '^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/\d{4}$' then to_date(btrim(baduta_tanggal_lahir), 'MM/DD/YYYY')
            else null
        end as baduta_tanggal_lahir,
        case when replace(btrim(coalesce(baduta_berat_badan, '')), ',', '.') ~ '^\d+(\.\d+)?$'
             then replace(btrim(baduta_berat_badan), ',', '.')::numeric end as baduta_berat_badan, -- weight
        case when replace(btrim(coalesce(baduta_tinggi_badan, '')), ',', '.') ~ '^\d+(\.\d+)?$'
             then replace(btrim(baduta_tinggi_badan), ',', '.')::numeric end as baduta_tinggi_badan, -- height
        nullif(btrim(catatan), '') as catatan, -- notes

        -- provenance from the ingestion layer
        nullif(btrim(source_tab), '') as source_tab,
        case when btrim(coalesce(source_row, '')) ~ '^\d+$' then btrim(source_row)::int end as source_row,
        nullif(btrim(loaded_at), '')::timestamp as loaded_at
    from {{ source('raw_sheets', 'register_posyandu_pemerintah_baduta') }}
),

-- keyed: duplicate detection.
-- The source contains ~950 byte-identical repeat rows; they are flagged here
-- rather than dropped so the count can be reconciled against the government file.
keyed as (
    select
        *,
        row_number() over (
            partition by source_tab, posyandu, baduta_nama, baduta_tanggal_lahir, kunjungan_tanggal
            order by source_row
        ) as dup_rank
    from typed
),

-- aged: derive the WHO lookup keys from the cleaned row.
-- births dated after the visit are rejected (2 such rows in the government file).
aged as (
    select
        *,
        case
            when kunjungan_tanggal is null or baduta_tanggal_lahir is null then null
            when baduta_tanggal_lahir > kunjungan_tanggal then null
            else (extract(year from age(kunjungan_tanggal, baduta_tanggal_lahir))::int * 12) + extract(month from age(kunjungan_tanggal, baduta_tanggal_lahir))::int
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

-- lms_joined: bring the WHO WFA LMS values and LHFA M/S values onto each child record.
lms_joined as (
    select
        a.*,
        w.l as wfa_l,
        w.m as wfa_m,
        w.s as wfa_s,
        h.m as hfa_m,
        h.s as hfa_s,
        h.l as hfa_l
    from aged a
    left join ref_wfa w on w.month = a.baduta_usia_bulan and w.sex = a.baduta_who_sex
    left join ref_lhfa h on h.month = a.baduta_usia_bulan and h.sex = a.baduta_who_sex
),

-- zscore_inputs:
--   WFA raw LMS z-score = ((y / M)^L - 1) / (S * L)
--   WFA WHO SD cutoff   = M * (1 + L * S * k)^(1 / L)
zscore_inputs as (
    select
        l.*,
        ((power(baduta_berat_badan / wfa_m, wfa_l) - 1) / (wfa_s * wfa_l)) as z_wfa_raw,
        wfa_m * power(1 + wfa_l * wfa_s *  3, 1.0 / wfa_l) as wfa_sd3pos,
        wfa_m * power(1 + wfa_l * wfa_s *  2, 1.0 / wfa_l) as wfa_sd2pos,
        wfa_m * power(1 + wfa_l * wfa_s * -3, 1.0 / wfa_l) as wfa_sd3neg,
        wfa_m * power(1 + wfa_l * wfa_s * -2, 1.0 / wfa_l) as wfa_sd2neg,
        ((power(baduta_tinggi_badan / hfa_m, hfa_l) - 1) / (hfa_s * hfa_l)) as z_hfa_raw,
        hfa_m * power(1 + hfa_l * hfa_s *  3, 1.0 / hfa_l) as hfa_sd3pos,
        hfa_m * power(1 + hfa_l * hfa_s *  2, 1.0 / hfa_l) as hfa_sd2pos,
        hfa_m * power(1 + hfa_l * hfa_s * -3, 1.0 / hfa_l) as hfa_sd3neg,
        hfa_m * power(1 + hfa_l * hfa_s * -2, 1.0 / hfa_l) as hfa_sd2neg
    from lms_joined l
),

-- zscored: keep the raw z-score inside +/-3 SD, otherwise switch to the WHO
-- linear tail adjustment using the +/-2 SD and +/-3 SD cutoffs.
-- https://cdn.who.int/media/docs/default-source/child-growth/growth-reference-5-19-years/computation.pdf?sfvrsn=c2ff6a95_4
zscored as (
    select
        z.*,
        round(case when z_wfa_raw > 3 then 3 + (baduta_berat_badan - wfa_sd3pos) / (wfa_sd3pos - wfa_sd2pos)
        when z_wfa_raw < -3 then -3 + (baduta_berat_badan - wfa_sd3neg) / (wfa_sd2neg - wfa_sd3neg)
        else z_wfa_raw end, 2) as z_wfa,
        round(case when z_hfa_raw > 3 then 3 + (baduta_tinggi_badan - hfa_sd3pos) / (hfa_sd3pos - hfa_sd2pos)
        when z_hfa_raw < -3 then -3 + (baduta_tinggi_badan - hfa_sd3neg) / (hfa_sd2neg - hfa_sd3neg)
        else z_hfa_raw end, 2) as z_hfa
    from zscore_inputs z
),

-- flagged: only the z-score checks are derivable here; the feeding checks
-- depend on asi/mpasi/protein binaries the government register does not carry.
flagged as (
    select
        z.*,
        case
            when z_wfa is null then null
            when z_wfa <= -2 then true
            else false
        end as baduta_waz_check,
        case
            when z_hfa is null then null
            when z_hfa <= -2 then true
            else false
        end as baduta_haz_check
    from zscored z
)

-- terminal select: same shape as register_posyandu_baduta_stg so the two can union.
select
    kunjungan_tanggal,

    -- not collected in the government register
    null::text as chw_nama_1, null::text as chw_nama_2, null::text as chw_nama_3,
    null::text as chw_nama_4, null::text as chw_nama_5, null::text as chw_nama_6,
    null::text as hw_nama_1, null::text as hw_nama_2, null::text as hw_nama_3,
    null::text as hw_nama_4, null::text as hw_nama_5,

    provinsi, kota_kabupaten, kecamatan, desa_kelurahan, puskesmas, posyandu,
    baduta_nama,
    baduta_gender,
    baduta_tanggal_lahir,
    baduta_usia_bulan,
    baduta_pengasuh_nama,

    -- not collected in the government register
    null::text as baduta_asi_biner,
    null::text as baduta_mpasi_biner,
    null::text as baduta_protein_biner,

    baduta_berat_badan,
    baduta_tinggi_badan,
    null::int as kr_jumlah,
    catatan,
    z_wfa as baduta_waz,
    z_hfa as baduta_haz,
    baduta_waz_check,
    baduta_haz_check,
    null::boolean as baduta_asi_check,     -- needs baduta_asi_biner / baduta_mpasi_biner
    null::boolean as baduta_protein_check, -- needs baduta_protein_biner
    null::boolean as baduta_wf_check,      -- if a child hasnt been weighed in 3 months

    -- provenance
    source_tab,
    source_row,
    loaded_at,
    dup_rank,
    (dup_rank > 1) as is_duplicate
from flagged