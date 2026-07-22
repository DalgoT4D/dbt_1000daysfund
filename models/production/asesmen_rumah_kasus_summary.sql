{{ config(materialized='table', tags=['production', 'parent', 'ACTIVEKunjungan_Rumah_Kasus']) }}

with baduta as (
    select distinct on (m.unified_name, r.kota_kabupaten, r.desa_kelurahan)
        m.unified_name, r.kota_kabupaten as district, r.desa_kelurahan as village,
        r.provinsi as province, r.kecamatan as subdistrict, r.puskesmas, r.posyandu,
        r.baduta_waz_check, r.baduta_haz_check,
        r.baduta_waz as latest_baduta_waz,
        r.baduta_haz as latest_baduta_haz,
        r.baduta_asi_check as latest_baduta_asi_check,
        r.baduta_protein_check as latest_baduta_protein_check
    from {{ ref('register_posyandu_baduta_stg') }} r
    join {{ ref('parent_name_fct') }} m on m.name_variant = r.baduta_pengasuh_nama
    order by m.unified_name, r.kota_kabupaten, r.desa_kelurahan,
        r.kunjungan_tanggal desc nulls last
),

bumil as (
    select distinct on (m.unified_name, r.kota_kabupaten, r.desa_kelurahan)
        m.unified_name, r.kota_kabupaten as district, r.desa_kelurahan as village,
        r.provinsi as province, r.kecamatan as subdistrict, r.puskesmas, r.posyandu,
        r.ibu_lila as latest_ibu_lila,
        r.ibu_kek_check as latest_ibu_kek_check,
        r.ibu_anc_check as latest_ibu_anc_check,
        r.ibu_hipertensi_check as latest_ibu_hipertensi_check,
        r.ibu_risti_check as latest_ibu_risti_check,
        r.ibu_ttd_check as latest_ibu_ttd_check
    from {{ ref('register_posyandu_bumil_stg') }} r
    join {{ ref('parent_name_fct') }} m on m.name_variant = r.ibu_nama
    order by m.unified_name, r.kota_kabupaten, r.desa_kelurahan,
        r.kunjungan_tanggal desc nulls last
),

posyandu_parents as (
    select
        unified_name, district, village,
        (array_agg(province) filter (where province is not null))[1] as province,
        (array_agg(subdistrict) filter (where subdistrict is not null))[1] as subdistrict,
        (array_agg(puskesmas) filter (where puskesmas is not null))[1] as puskesmas,
        (array_agg(posyandu) filter (where posyandu is not null))[1] as posyandu
    from (
        select unified_name, province, district, subdistrict, village, puskesmas, posyandu from baduta
        union all
        select unified_name, province, district, subdistrict, village, puskesmas, posyandu from bumil
    ) registrations
    group by unified_name, district, village
),

followups as (
    select
        m.unified_name, r.provinsi as province, r.kota_kabupaten as district,
        r.kecamatan as subdistrict, r.desa_kelurahan as village,
        count(distinct r.submission_id) as followup_count,
        min(r.kunjungan_tanggal) as first_followup_date,
        max(r.kunjungan_tanggal) as latest_followup_date,
        max(r.kunjungan_ke) as latest_reported_visit_number,
        string_agg(distinct r.responden_kasus_baduta, ' | ') as case_types,
        count(*) filter (where r.rujukan_pkm = 1) as puskesmas_referral_count,
        count(*) filter (where r.rujukan_pmt = 1) as pmt_referral_count
    from {{ ref('asesmen_kunjungan_rumah_kasus') }} r
    join {{ ref('parent_name_fct') }} m on m.name_variant = r.pengasuh_nama
    group by m.unified_name, r.provinsi, r.kota_kabupaten, r.kecamatan, r.desa_kelurahan
)

select
    concat_ws('|', {{ profile_name_key('p.unified_name') }},
        {{ profile_name_key('p.district') }}, {{ profile_name_key('p.village') }}) as parent_key,
    p.unified_name as parent_name,
    p.province, p.district, p.subdistrict, p.village, p.puskesmas, p.posyandu,
    b.unified_name is not null as is_registered_baduta,
    u.unified_name is not null as is_registered_bumil,
    case
        when b.unified_name is not null and u.unified_name is not null then 'baduta_and_bumil'
        when b.unified_name is not null then 'baduta'
        when u.unified_name is not null then 'bumil'
    end as registration_status,
    b.baduta_waz_check,
    b.baduta_haz_check,
    b.latest_baduta_waz,
    b.latest_baduta_haz,
    b.latest_baduta_asi_check,
    b.latest_baduta_protein_check,
    u.latest_ibu_lila,
    u.latest_ibu_kek_check,
    u.latest_ibu_anc_check,
    u.latest_ibu_hipertensi_check,
    u.latest_ibu_risti_check,
    u.latest_ibu_ttd_check,
    coalesce(f.followup_count, 0) as followup_count,
    f.first_followup_date,
    f.latest_followup_date,
    f.latest_reported_visit_number,
    f.case_types,
    coalesce(f.puskesmas_referral_count, 0) as puskesmas_referral_count,
    coalesce(f.pmt_referral_count, 0) as pmt_referral_count
from posyandu_parents p
left join baduta b on p.unified_name = b.unified_name
    and p.district is not distinct from b.district and p.village is not distinct from b.village
left join bumil u on p.unified_name = u.unified_name
    and p.district is not distinct from u.district and p.village is not distinct from u.village
left join followups f on p.unified_name = f.unified_name
    and {{ profile_name_key('p.district') }} is not distinct from {{ profile_name_key('f.district') }}
    and {{ profile_name_key('p.village') }} is not distinct from {{ profile_name_key('f.village') }}
