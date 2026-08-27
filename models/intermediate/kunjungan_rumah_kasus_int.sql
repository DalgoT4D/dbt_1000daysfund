-- Model: Links home-visit assessments to canonical parent identities.
{{ config(materialized='table', tags=['intermediate', 'parent', 'ACTIVEKunjungan_Rumah_Kasus']) }}

select
    md5(concat_ws('|',
        {{ profile_name_key('m.unified_name') }},
        {{ profile_name_key('r.kota_kabupaten') }},
        {{ profile_name_key('r.kecamatan') }},
        {{ profile_name_key('r.desa_kelurahan') }}
    )) as parent_id,
    initcap(m.unified_name) as parent_name,
    r.provinsi,
    r.kota_kabupaten,
    r.kecamatan,
    r.desa_kelurahan,
    r.puskesmas,
    r.posyandu,
    r.kunjungan_tanggal,
    r.enumerator_peran,
    r.enumerator_nama,
    r.chw_nama,
    r.pendamping_nama,
    r.pendamping_peran,
    r.jenis_kunjungan,
    r.pengasuh_nama,
    r.responden_nama,
    r.responden_kategori,
    r.responden_kasus,
    r.rujukan_pkm,
    r.rujukan_pmt_layak,
    r.rujukan_dapat_pmt,
    r.durasi_kunjungan

from {{ ref('active_kunjungan_rumah_kasus') }} r
join {{ ref('parent_name_fct') }} m on m.name_variant = r.pengasuh_nama