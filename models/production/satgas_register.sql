-- Model: Satgas home visits linked to the Posyandu registers.
-- Grain: one row per Satgas visit record (satgas_id), same as satgas_kunjungan_int.
--
-- Step 1, person link: each Satgas person (src_person_id) is matched to at most one
--   register person (person_id from register_posyandu_combined_int), within the same
--   kabupaten and responden_kategori. All name spellings seen for a person on either
--   side are compared and the best score is used. Desa names are compared without the
--   desa/kelurahan prefix and without spaces, since the two sources write them differently.
--     R1 same name + same desa
--     R2 name similarity >= 0.6 + desa similarity >= 0.6
--     R3 baduta only: name similarity >= 0.5 + caregiver (pengasuh) similarity >= 0.6
--     R4 same name, desa differs or is missing (weakest; check before relying on it)
--   A pair is kept only when each side is the other's best candidate (one-to-one).
--
-- Step 2, same month: when the linked register person has a row in the Satgas visit's
--   month (kunjungan_bulan), its clinical and behavioral values are pulled into reg_*
--   columns. Register values follow production.register_posyandu_combined: the TDF value
--   when present, otherwise the pemerintah value. Behavioral fields (ASI, MPASI, protein,
--   TTD, ANC) only exist in the TDF register.
--
-- diff_* = Satgas value minus register value (same-month rows only).
-- days_between_visits = Satgas visit date minus register visit date.
-- Meta and linkage columns are right-most.
-- Requires pg_trgm.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    tags=['satgas', 'register_posyandu', 'intermediate']
) }}

with recursive

satgas as (
    select * from {{ ref('satgas_kunjungan_int') }}
),

register as (
    select * from {{ ref('register_posyandu_combined_int') }}
),

-- Satgas name variants per person.
s_variants as (
    select distinct
        src_person_id,
        responden_kategori,
        {{ profile_name_key('kota_kabupaten') }}                                as kab_key,
        -- desa without 'desa'/'kelurahan'/'kel' prefix and spaces: 'KEL NIKI - NIKI' = 'Niki-Niki', 'Sama Guna' = 'SAMAGUNA'
        replace(regexp_replace({{ profile_name_key('desa_kelurahan') }}, '^(desa|ds|kelurahan|kel)\s+', ''), ' ', '') as desa_key,
        {{ profile_name_key('coalesce(baduta_nama, ibu_nama)') }}               as name_key,
        {{ profile_name_key('baduta_pengasuh_nama') }}                          as pengasuh_key
    from satgas
    where src_person_id is not null
),

-- Register name variants per person (both sources' spellings).
r_names as (
    select person_id, responden_kategori, kota_kabupaten, desa_kelurahan,
           coalesce(pem_baduta_nama, pem_ibu_nama) as nama, pem_pengasuh_nama as pengasuh
    from register
    union all
    select person_id, responden_kategori, kota_kabupaten, desa_kelurahan,
           coalesce(tdf_baduta_nama, tdf_ibu_nama), tdf_pengasuh_nama
    from register
),

r_variants as (
    select distinct
        person_id,
        responden_kategori,
        {{ profile_name_key('kota_kabupaten') }}                                as kab_key,
        -- desa without 'desa'/'kelurahan'/'kel' prefix and spaces: 'KEL NIKI - NIKI' = 'Niki-Niki', 'Sama Guna' = 'SAMAGUNA'
        replace(regexp_replace({{ profile_name_key('desa_kelurahan') }}, '^(desa|ds|kelurahan|kel)\s+', ''), ' ', '') as desa_key,
        {{ profile_name_key('nama') }}                                          as name_key,
        {{ profile_name_key('pengasuh') }}                                      as pengasuh_key
    from r_names
    where nama is not null
),

-- Variant-level candidates, then the best evidence per person pair.
pair_evidence as (
    select
        s.src_person_id,
        r.person_id,
        s.responden_kategori,
        max(similarity(s.name_key, r.name_key))                                  as name_sim,
        bool_or(s.name_key = r.name_key)                                         as same_name,
        bool_or(s.name_key = r.name_key and s.desa_key = r.desa_key)             as same_name_desa,
        max(case when similarity(s.desa_key, r.desa_key) >= 0.6
                 then similarity(s.name_key, r.name_key) end)                    as name_sim_desa,
        max(case when similarity(s.pengasuh_key, r.pengasuh_key) >= 0.6
                 then similarity(s.name_key, r.name_key) end)                    as name_sim_pengasuh
    from s_variants s
    join r_variants r
        on  s.responden_kategori = r.responden_kategori
        and s.kab_key = r.kab_key
        and (s.name_key = r.name_key or similarity(s.name_key, r.name_key) >= 0.5)
    group by 1, 2, 3
),

candidates as (
    select
        *,
        case
            when same_name_desa                                                   then 1
            when name_sim_desa >= 0.6                                             then 2
            when responden_kategori = 'baduta' and name_sim_pengasuh >= 0.5      then 3
            when same_name                                                        then 4
        end as tier
    from pair_evidence
),

ranked as (
    select
        *,
        row_number() over (partition by src_person_id order by tier, name_sim desc, person_id)     as rn_s,
        row_number() over (partition by person_id     order by tier, name_sim desc, src_person_id) as rn_r
    from candidates
    where tier is not null
),

links as (
    select
        src_person_id,
        person_id                                       as register_person_id,
        'R' || tier                                     as register_match_tier,
        round(name_sim::numeric, 2)                     as register_name_sim
    from ranked
    where rn_s = 1 and rn_r = 1
),

-- Register values per person-month, TDF preferred over pemerintah.
reg_month as (
    select
        person_id,
        kunjungan_bulan,
        coalesce(tdf_kunjungan_tanggal,    pem_kunjungan_tanggal)      as reg_kunjungan_tanggal,
        coalesce(tdf_baduta_nama,          pem_baduta_nama)            as reg_baduta_nama,
        coalesce(tdf_ibu_nama,             pem_ibu_nama)               as reg_ibu_nama,
        coalesce(tdf_baduta_tanggal_lahir, pem_baduta_tanggal_lahir)   as reg_baduta_tanggal_lahir,

        -- bumil clinical
        coalesce(tdf_ibu_usia_kehamilan_minggu, pem_ibu_usia_kehamilan_minggu) as reg_ibu_usia_kehamilan_minggu,
        coalesce(tdf_ibu_trimester,        pem_ibu_trimester)          as reg_ibu_trimester,
        coalesce(tdf_ibu_lila,             pem_ibu_lila)               as reg_ibu_lila,
        coalesce(tdf_ibu_sistolik,         pem_ibu_sistolik)           as reg_ibu_sistolik,
        coalesce(tdf_ibu_diastolik,        pem_ibu_diastolik)          as reg_ibu_diastolik,
        coalesce(tdf_ibu_kek_check,        pem_ibu_kek_check)          as reg_ibu_kek_check,
        coalesce(tdf_ibu_hipertensi_check, pem_ibu_hipertensi_check)   as reg_ibu_hipertensi_check,

        -- bumil behavioral (TDF only)
        ibu_ttd_perc                                                   as reg_ibu_ttd_perc,
        ibu_ttd_check                                                  as reg_ibu_ttd_check,
        ibu_anc_jumlah                                                 as reg_ibu_anc_jumlah,
        ibu_anc_check                                                  as reg_ibu_anc_check,

        -- baduta clinical
        coalesce(tdf_baduta_usia_bulan,    pem_baduta_usia_bulan)      as reg_baduta_usia_bulan,
        coalesce(tdf_baduta_berat_badan,   pem_baduta_berat_badan)     as reg_baduta_berat_badan,
        coalesce(tdf_baduta_tinggi_badan,  pem_baduta_tinggi_badan)    as reg_baduta_tinggi_badan,
        coalesce(tdf_baduta_waz,           pem_baduta_waz)             as reg_baduta_waz,
        coalesce(tdf_baduta_haz,           pem_baduta_haz)             as reg_baduta_haz,
        coalesce(tdf_baduta_waz_check,     pem_baduta_waz_check)       as reg_baduta_waz_check,
        coalesce(tdf_baduta_haz_check,     pem_baduta_haz_check)       as reg_baduta_haz_check,
        coalesce(tdf_baduta_wf_check,      pem_baduta_wf_check)        as reg_baduta_wf_check,

        -- baduta behavioral (TDF only)
        baduta_asi_biner                                               as reg_baduta_asi_biner,
        baduta_mpasi_biner                                             as reg_baduta_mpasi_biner,
        baduta_protein_biner                                           as reg_baduta_protein_biner,
        baduta_asi_check                                               as reg_baduta_asi_check,
        baduta_protein_check                                           as reg_baduta_protein_check,

        in_pemerintah                                                  as reg_in_pemerintah,
        in_tdf                                                         as reg_in_tdf
    from register
)

select
    -- Satgas visit
    s.responden_kategori,
    s.kota_kabupaten,
    s.puskesmas,
    s.desa_kelurahan,
    s.posyandu,
    s.kunjungan_bulan_raw,
    s.kunjungan_bulan,
    s.kunjungan_tanggal,
    s.year,
    s.quarter,
    s.kunjungan_ke,
    s.kunjungan_jumlah,
    s.kasus_raw,
    s.kasus_status,
    s.masalah_utama,
    s.chw_nama_raw,
    s.satgas_nama_raw,
    s.ibu_nama,
    s.ibu_suami_nama,
    s.ibu_usia_kehamilan_raw,
    s.ibu_berat_badan,
    s.ibu_lila,
    s.ibu_tekanan_darah_raw,
    s.ibu_tekanan_darah_sistolik,
    s.ibu_tekanan_darah_diastolik,
    s.ibu_tekanan_darah_flag,
    s.baduta_nama,
    s.baduta_pengasuh_nama,
    s.baduta_usia_raw,
    s.baduta_berat_badan,
    s.baduta_tinggi_badan,
    s.penyuluhan,
    s.progres,
    s.catatan,

    -- register, same month
    rm.reg_kunjungan_tanggal,
    s.kunjungan_tanggal - rm.reg_kunjungan_tanggal                         as days_between_visits,
    rm.reg_baduta_nama,
    rm.reg_ibu_nama,
    rm.reg_baduta_tanggal_lahir,
    rm.reg_ibu_usia_kehamilan_minggu,
    rm.reg_ibu_trimester,
    rm.reg_ibu_lila,
    rm.reg_ibu_sistolik,
    rm.reg_ibu_diastolik,
    rm.reg_ibu_kek_check,
    rm.reg_ibu_hipertensi_check,
    rm.reg_ibu_ttd_perc,
    rm.reg_ibu_ttd_check,
    rm.reg_ibu_anc_jumlah,
    rm.reg_ibu_anc_check,
    rm.reg_baduta_usia_bulan,
    rm.reg_baduta_berat_badan,
    rm.reg_baduta_tinggi_badan,
    rm.reg_baduta_waz,
    rm.reg_baduta_haz,
    rm.reg_baduta_waz_check,
    rm.reg_baduta_haz_check,
    rm.reg_baduta_wf_check,
    rm.reg_baduta_asi_biner,
    rm.reg_baduta_mpasi_biner,
    rm.reg_baduta_protein_biner,
    rm.reg_baduta_asi_check,
    rm.reg_baduta_protein_check,

    -- comparison, Satgas minus register
    s.baduta_berat_badan  - rm.reg_baduta_berat_badan                     as diff_baduta_berat_badan,
    s.baduta_tinggi_badan - rm.reg_baduta_tinggi_badan                    as diff_baduta_tinggi_badan,
    s.ibu_lila - rm.reg_ibu_lila                                          as diff_ibu_lila,
    s.ibu_tekanan_darah_sistolik  - rm.reg_ibu_sistolik                   as diff_ibu_sistolik,
    s.ibu_tekanan_darah_diastolik - rm.reg_ibu_diastolik                  as diff_ibu_diastolik,

    -- meta
    s.source_file,
    s.source_row,
    s.satgas_id,
    s.src_person_id,
    s.unified_name,
    s.unified_name_key,
    l.register_person_id,
    l.register_match_tier,
    l.register_name_sim,
    l.register_person_id is not null                                       as in_register,
    rm.person_id is not null                                               as register_same_month,
    rm.reg_in_pemerintah,
    rm.reg_in_tdf

from satgas s
left join links l
    on l.src_person_id = s.src_person_id
left join reg_month rm
    on  rm.person_id = l.register_person_id
    and rm.kunjungan_bulan = s.kunjungan_bulan