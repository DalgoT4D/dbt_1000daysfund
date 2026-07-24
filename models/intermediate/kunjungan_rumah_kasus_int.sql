{{ config(materialized='table', tags=['intermediate', 'parent', 'ACTIVEKunjungan_Rumah_Kasus']) }}

select
    md5(concat_ws('|',
        {{ profile_name_key('m.unified_name') }},
        {{ profile_name_key('r.kota_kabupaten') }},
        {{ profile_name_key('r.kecamatan') }},
        {{ profile_name_key('r.desa_kelurahan') }}
    )) as parent_id,
    initcap(m.unified_name) as parent_name,
    r.submission_id,
    r.submission_time,
    r.geolocation_latitude,
    r.geolocation_longitude,
    r.attachments,
    r.provinsi,
    r.kota_kabupaten,
    r.kecamatan,
    r.desa_kelurahan,
    r.puskesmas,
    r.posyandu_kode,
    r.kunjungan_tanggal,
    r.enumerator_peran,
    r.enumerator_nama_lain,
    r.chw_nama_lain,
    r.pendamping_lain,
    r.pendamping_nama,
    r.pendamping_peran,
    r.kunjungan_ke,
    r.pengasuh_nama,
    r.baduta_nama_lain,
    r.responden_kategori,
    r.responden_kasus_baduta,
    r.rujukan_pkm,
    r.rujukan_pmt,
    r.refleksi_masalah_audio,
    r.refleksi_dukungan_audio,
    r.refleksi_kunjungan_audio,
    r.dokumentasi_file,
    r.penutup_geolokasi,
    r.penutup_durasi

from {{ ref('asesmen_kunjungan_rumah_kasus') }} r
join {{ ref('parent_name_fct') }} m on m.name_variant = r.pengasuh_nama
