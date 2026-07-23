{{ config(materialized='table', tags=['production', 'parent', 'ACTIVEKunjungan_Rumah_Kasus']) }}

select
    concat_ws('|', {{ profile_name_key('m.unified_name') }},
        {{ profile_name_key('r.kota_kabupaten') }}, {{ profile_name_key('r.kecamatan') }},
        {{ profile_name_key('r.desa_kelurahan') }}) as parent_key,
    m.unified_name as parent_name,
    r.provinsi as province,
    r.kota_kabupaten as district,
    r.kecamatan as subdistrict,
    r.desa_kelurahan as village,
    (array_agg(r.puskesmas order by r.kunjungan_tanggal desc) filter (where r.puskesmas is not null))[1] as puskesmas,
    (array_agg(r.posyandu_kode order by r.kunjungan_tanggal desc) filter (where r.posyandu_kode is not null))[1] as posyandu,
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
