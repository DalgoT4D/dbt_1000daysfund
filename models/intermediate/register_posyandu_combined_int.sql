-- Model: Person-level linkage of the government (general population) and TDF
-- (program-visited) Posyandu registers, at the person x month grain.
--
-- Persons are first resolved WITHIN each source (src_person_id / unified_name come from
-- register_posyandu_pemerintah_int and register_posyandu_tdf_int). This model only
-- links those within-source persons ACROSS sources and assigns the shared person_id.
--
-- Grain: one row per person per calendar month (kunjungan_bulan) with a visit in either source.
--   * Same person, same month in both sources -> ONE row, pemerintah values in pem_*
--     columns and TDF values in tdf_* columns side by side.
--   * Otherwise the row carries only one side and is flagged via match_status.
--
-- match_status:
--   both_same_month   person linked across sources AND visited in both this month
--   both_other_month  person linked across sources, but only one source has a visit this month
--   pemerintah_only   person never found in TDF register
--   tdf_only          person never found in pemerintah register
--
-- person_id: md5 of the linked pair of src_person_ids, or the person's own
-- src_person_id when unlinked. Only person_id is comparable across sources.
--
-- Cross-source linkage (one-to-one, within kabupaten and responden_kategori), on
-- unified names:
--   baduta  B1 exact name + same DOB
--           B2 same DOB + name similarity >= 0.5
--           B3 same DOB + abbreviated name: similarity >= 0.3, same last name and same
--              first initial, or similarity >= 0.4 and same first name
--              ("MARIA Y N NITU" ~ "MARIA YASINTA NONA NITU")
--           B4 name similarity >= 0.8 + DOB within 31 days + same desa or posyandu
--   bumil   M1 exact name + same desa
--           M2 name similarity >= 0.8 + (same desa or HPHT within 28 days)
--           M3 exact name + reported age within 2 years
-- Each person keeps only its best candidate, and a pair is kept only when each side
-- is the other's best candidate, so no person is linked twice.
-- Within a source, a person with several visits in one month keeps the latest visit.
-- Requires pg_trgm (similarity()).
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    tags=['intermediate', 'register_posyandu']
) }}

with

-- Both registers, CHW/HW name columns dropped, plus matching keys.
visits as (
    {% for src, model in [('pem', 'register_posyandu_pemerintah_int'), ('tdf', 'register_posyandu_tdf_int')] %}
    select
        '{{ src }}'::text                                         as src,
        src_person_id,
        unified_name,
        unified_name_key,
        responden_kategori,
        kunjungan_tanggal,
        date_trunc('month', kunjungan_tanggal)::date              as visit_month,
        provinsi, kota_kabupaten, kecamatan, desa_kelurahan, puskesmas, posyandu,
        kr_jumlah, catatan,
        ibu_nama, ibu_usia, ibu_hpht,
        ibu_tekanan_darah_sistolik, ibu_tekanan_darah_diastolik, ibu_lila,
        ibu_ttd_mulai, ibu_ttd_jumlah_raw, ibu_anc_jumlah,
        ibu_usia_kehamilan_minggu, ibu_trimester, ibu_ttd_perc,
        ibu_kek_check, ibu_anc_check, ibu_hipertensi_check, ibu_risti_check, ibu_ttd_check,
        baduta_nama, baduta_gender, baduta_tanggal_lahir, baduta_usia_bulan,
        baduta_pengasuh_nama, baduta_asi_biner, baduta_mpasi_biner, baduta_protein_biner,
        baduta_berat_badan, baduta_tinggi_badan, baduta_waz, baduta_haz,
        baduta_waz_check, baduta_haz_check, baduta_asi_check, baduta_protein_check,
        baduta_bb_naik_gram, baduta_kbm_gram, baduta_wf_check,
        kasus_check, kasus,
        {{ profile_name_key('kota_kabupaten') }}                  as kab_key,
        {{ profile_name_key('desa_kelurahan') }}                  as desa_key,
        {{ profile_name_key('posyandu') }}                        as posyandu_key
    from {{ ref(model) }}
    where kunjungan_tanggal is not null
      and src_person_id is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

-- One profile per within-source person, used only for matching.
persons as (
    select
        src_person_id,
        src,
        responden_kategori,
        max(unified_name_key)                                     as name_key,
        split_part(max(unified_name_key), ' ', 1)                 as first_token,
        regexp_replace(max(unified_name_key), '^.* ', '')         as last_token,
        mode() within group (order by kab_key)                    as kab_key,
        mode() within group (order by desa_key)                   as desa_key,
        mode() within group (order by posyandu_key)               as posyandu_key,
        mode() within group (order by baduta_tanggal_lahir)       as dob,
        max(ibu_hpht)                                             as hpht,
        max(ibu_usia)                                             as usia
    from visits
    group by 1, 2, 3
),

-- Candidate pairs within kabupaten + category, scored into tiers.
candidates as (
    select
        p.src_person_id                    as pem_person_id,
        t.src_person_id                    as tdf_person_id,
        p.responden_kategori,
        similarity(p.name_key, t.name_key) as name_sim,
        case
            when p.responden_kategori = 'baduta' then case
                when p.name_key = t.name_key and p.dob = t.dob                       then 1
                when p.dob = t.dob and similarity(p.name_key, t.name_key) >= 0.5     then 2
                when p.dob = t.dob
                     and (   (similarity(p.name_key, t.name_key) >= 0.3
                              and p.last_token = t.last_token
                              and left(p.first_token, 1) = left(t.first_token, 1))
                          or (similarity(p.name_key, t.name_key) >= 0.4
                              and p.first_token = t.first_token))                    then 3
                when similarity(p.name_key, t.name_key) >= 0.8
                     and abs(p.dob - t.dob) <= 31
                     and (p.desa_key = t.desa_key or p.posyandu_key = t.posyandu_key) then 4
            end
            else case
                when p.name_key = t.name_key and p.desa_key = t.desa_key             then 1
                when similarity(p.name_key, t.name_key) >= 0.8
                     and (p.desa_key = t.desa_key or abs(p.hpht - t.hpht) <= 28)     then 2
                when p.name_key = t.name_key and abs(p.usia - t.usia) <= 2           then 3
            end
        end                                as tier
    from persons p
    join persons t
        on  p.src = 'pem' and t.src = 'tdf'
        and p.responden_kategori = t.responden_kategori
        and p.kab_key = t.kab_key
        and (p.name_key = t.name_key
             or p.dob = t.dob
             or similarity(p.name_key, t.name_key) >= 0.3)
),

-- Keep a pair only if each side is the other's best candidate.
ranked as (
    select
        *,
        row_number() over (partition by pem_person_id order by tier, name_sim desc, tdf_person_id) as rn_pem,
        row_number() over (partition by tdf_person_id order by tier, name_sim desc, pem_person_id) as rn_tdf
    from candidates
    where tier is not null
),

links as (
    select
        pem_person_id,
        tdf_person_id,
        case when responden_kategori = 'baduta' then 'B' else 'M' end || tier as match_tier,
        round(name_sim::numeric, 2)                                           as name_sim
    from ranked
    where rn_pem = 1 and rn_tdf = 1
),

-- Map every within-source person to the cross-source person_id.
person_map as (
    select
        v.src_person_id,
        coalesce(md5(l.pem_person_id || '|' || l.tdf_person_id), v.src_person_id) as person_id,
        l.match_tier,
        l.name_sim,
        l.pem_person_id is not null                                                 as is_linked
    from persons v
    left join links l
        on (v.src = 'pem' and l.pem_person_id = v.src_person_id)
        or (v.src = 'tdf' and l.tdf_person_id = v.src_person_id)
),

-- Latest visit per person per month within each source.
person_month as (
    select
        v.*,
        m.person_id, m.match_tier, m.name_sim, m.is_linked,
        row_number() over (
            partition by v.src, m.person_id, v.visit_month
            order by v.kunjungan_tanggal desc
        ) as rn
    from visits v
    join person_map m using (src_person_id)
),

pem as (select * from person_month where src = 'pem' and rn = 1),
tdf as (select * from person_month where src = 'tdf' and rn = 1)

select
    coalesce(p.responden_kategori, t.responden_kategori)                 as responden_kategori,
    coalesce(p.visit_month, t.visit_month)                               as kunjungan_bulan,
    p.kunjungan_tanggal                                                  as pem_kunjungan_tanggal,
    t.kunjungan_tanggal                                                  as tdf_kunjungan_tanggal,
    extract(year from coalesce(p.visit_month, t.visit_month))::int       as year,
    extract(year from coalesce(p.visit_month, t.visit_month))::int || '-Q'
        || extract(quarter from coalesce(p.visit_month, t.visit_month))::int as quarter,
    coalesce(p.match_tier, t.match_tier)                                 as match_tier,

    -- location: TDF field visit preferred, puskesmas register as fallback
    coalesce(t.provinsi,       p.provinsi)                               as provinsi,
    coalesce(t.kota_kabupaten, p.kota_kabupaten)                         as kota_kabupaten,
    coalesce(t.kecamatan,      p.kecamatan)                              as kecamatan,
    coalesce(t.desa_kelurahan, p.desa_kelurahan)                         as desa_kelurahan,
    coalesce(t.puskesmas,      p.puskesmas)                              as puskesmas,
    coalesce(t.posyandu,       p.posyandu)                               as posyandu,

    -- identity
    p.ibu_nama               as pem_ibu_nama,             t.ibu_nama               as tdf_ibu_nama,
    p.baduta_nama            as pem_baduta_nama,          t.baduta_nama            as tdf_baduta_nama,
    p.baduta_pengasuh_nama   as pem_pengasuh_nama,        t.baduta_pengasuh_nama   as tdf_pengasuh_nama,
    p.baduta_gender          as pem_baduta_gender,        t.baduta_gender          as tdf_baduta_gender,
    p.baduta_tanggal_lahir   as pem_baduta_tanggal_lahir, t.baduta_tanggal_lahir   as tdf_baduta_tanggal_lahir,

    -- visits
    t.kunjungan_tanggal - p.kunjungan_tanggal                            as days_between_visits,

    -- bumil clinical (both sources)
    p.ibu_usia                    as pem_ibu_usia,        t.ibu_usia                    as tdf_ibu_usia,
    p.ibu_hpht                    as pem_ibu_hpht,        t.ibu_hpht                    as tdf_ibu_hpht,
    p.ibu_usia_kehamilan_minggu   as pem_ibu_usia_kehamilan_minggu,
    t.ibu_usia_kehamilan_minggu   as tdf_ibu_usia_kehamilan_minggu,
    p.ibu_trimester               as pem_ibu_trimester,   t.ibu_trimester               as tdf_ibu_trimester,
    p.ibu_lila                    as pem_ibu_lila,        t.ibu_lila                    as tdf_ibu_lila,
    p.ibu_tekanan_darah_sistolik  as pem_ibu_sistolik,    t.ibu_tekanan_darah_sistolik  as tdf_ibu_sistolik,
    p.ibu_tekanan_darah_diastolik as pem_ibu_diastolik,   t.ibu_tekanan_darah_diastolik as tdf_ibu_diastolik,
    p.ibu_kek_check               as pem_ibu_kek_check,   t.ibu_kek_check               as tdf_ibu_kek_check,
    p.ibu_hipertensi_check        as pem_ibu_hipertensi_check,
    t.ibu_hipertensi_check        as tdf_ibu_hipertensi_check,
    p.ibu_risti_check             as pem_ibu_risti_check, t.ibu_risti_check             as tdf_ibu_risti_check,

    -- bumil behavioral (TDF only)
    t.ibu_ttd_mulai, t.ibu_ttd_jumlah_raw, t.ibu_ttd_perc, t.ibu_ttd_check,
    t.ibu_anc_jumlah, t.ibu_anc_check,

    -- baduta clinical (both sources)
    p.baduta_usia_bulan    as pem_baduta_usia_bulan,    t.baduta_usia_bulan    as tdf_baduta_usia_bulan,
    p.baduta_berat_badan   as pem_baduta_berat_badan,   t.baduta_berat_badan   as tdf_baduta_berat_badan,
    p.baduta_tinggi_badan  as pem_baduta_tinggi_badan,  t.baduta_tinggi_badan  as tdf_baduta_tinggi_badan,
    p.baduta_waz           as pem_baduta_waz,           t.baduta_waz           as tdf_baduta_waz,
    p.baduta_haz           as pem_baduta_haz,           t.baduta_haz           as tdf_baduta_haz,
    p.baduta_waz_check     as pem_baduta_waz_check,     t.baduta_waz_check     as tdf_baduta_waz_check,
    p.baduta_haz_check     as pem_baduta_haz_check,     t.baduta_haz_check     as tdf_baduta_haz_check,

    -- weight faltering, judged within each source's own monthly series
    p.baduta_bb_naik_gram  as pem_baduta_bb_naik_gram,  t.baduta_bb_naik_gram  as tdf_baduta_bb_naik_gram,
    p.baduta_kbm_gram      as pem_baduta_kbm_gram,      t.baduta_kbm_gram      as tdf_baduta_kbm_gram,
    p.baduta_wf_check      as pem_baduta_wf_check,      t.baduta_wf_check      as tdf_baduta_wf_check,

    -- baduta behavioral (TDF only)
    t.baduta_asi_biner, t.baduta_mpasi_biner, t.baduta_protein_biner,
    t.baduta_asi_check, t.baduta_protein_check,

    -- measurement comparison, TDF minus pemerintah (populated on both_same_month rows)
    t.baduta_berat_badan  - p.baduta_berat_badan                         as diff_berat_badan,
    t.baduta_tinggi_badan - p.baduta_tinggi_badan                        as diff_tinggi_badan,
    t.baduta_waz - p.baduta_waz                                          as diff_waz,
    t.baduta_haz - p.baduta_haz                                          as diff_haz,
    t.ibu_lila - p.ibu_lila                                              as diff_lila,
    t.ibu_tekanan_darah_sistolik  - p.ibu_tekanan_darah_sistolik         as diff_sistolik,
    t.ibu_tekanan_darah_diastolik - p.ibu_tekanan_darah_diastolik        as diff_diastolik,

    -- per-source case flags and notes
    p.kasus_check as pem_kasus_check, p.kasus as pem_kasus,
    t.kasus_check as tdf_kasus_check, t.kasus as tdf_kasus,
    t.kr_jumlah   as tdf_kr_jumlah,
    p.catatan     as pem_catatan,     t.catatan     as tdf_catatan,

    -- linkage
    coalesce(p.person_id, t.person_id)                                   as person_id,
    p.src_person_id                                                      as pem_src_person_id,
    t.src_person_id                                                      as tdf_src_person_id,
    case
        when p.person_id is not null and t.person_id is not null then 'both_same_month'
        when coalesce(p.is_linked, t.is_linked)                    then 'both_other_month'
        when p.person_id is not null                               then 'pemerintah_only'
        else 'tdf_only'
    end                                                                  as match_status,
    coalesce(p.name_sim, t.name_sim)                                     as name_sim,
    p.person_id is not null                                              as in_pemerintah,
    t.person_id is not null                                              as in_tdf

from pem p
full outer join tdf t
    on p.person_id = t.person_id and p.visit_month = t.visit_month