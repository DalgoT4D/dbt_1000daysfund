{{ config(materialized='table') }}

with source_data as (
    select case when data is null or trim(data::text) = '' then null::jsonb else data::jsonb end as json_payload
    from {{ source('raw_kobo', 'ACTIVEKunjungan_Rumah_Kasus') }}
),

typed_data as (
    select
        nullif(json_payload ->> '_id', '')::bigint as submission_id,
        nullif(json_payload ->> '_uuid', '') as submission_uuid,
        nullif(json_payload ->> '_status', '') as submission_status,
        coalesce(nullif(json_payload ->> '__version__', ''), nullif(json_payload ->> '**version**', '')) as submission_version,
        nullif(json_payload ->> '_submitted_by', '') as submitted_by,
        nullif(json_payload ->> '_xform_id_string', '') as xform_id_string,
        nullif(json_payload ->> 'formhub/uuid', '') as formhub_uuid,
        nullif(json_payload ->> 'meta/rootUuid', '') as meta_root_uuid,
        nullif(json_payload ->> 'meta/instanceID', '') as meta_instance_id,
        nullif(json_payload ->> 'start', '')::timestamptz as submission_start_at,
        nullif(json_payload ->> 'end', '')::timestamptz as submission_end_at,
        nullif(json_payload ->> '_submission_time', '')::timestamp as submission_time,
        nullif(json_payload #>> '{_geolocation,0}', '')::numeric as geolocation_latitude,
        nullif(json_payload #>> '{_geolocation,1}', '')::numeric as geolocation_longitude,
        json_payload -> '_attachments' as attachments,
        json_payload -> '_validation_status' as validation_status,

        nullif(btrim(json_payload ->> 'pembukaan/provinsi'), '') as provinsi_kode,
        nullif(btrim(json_payload ->> 'pembukaan/kota_kabupaten'), '') as kota_kabupaten_kode,
        nullif(btrim(json_payload ->> 'pembukaan/kecamatan'), '') as kecamatan_kode,
        nullif(btrim(json_payload ->> 'pembukaan/desa_kelurahan'), '') as desa_kelurahan_kode,
        nullif(btrim(json_payload ->> 'pembukaan/puskesmas'), '') as puskesmas_kode,
        nullif(btrim(json_payload ->> 'pembukaan/posyandu'), '') as posyandu_kode,
        {{ validate_date("(json_payload ->> 'pembukaan/kunjungan_tanggal')") }} as kunjungan_tanggal,
        nullif(btrim(json_payload ->> 'pembukaan/enumerator_peran'), '') as enumerator_peran,
        nullif(btrim(json_payload ->> 'pembukaan/enumerator_nama_lain'), '') as enumerator_nama_lain,
        nullif(btrim(json_payload ->> 'pembukaan/chw_nama_lain'), '') as chw_nama_lain,
        nullif(btrim(json_payload ->> 'pembukaan/pendamping_lain'), '') as pendamping_lain,
        nullif(btrim(json_payload ->> 'pembukaan/pendamping_nama'), '') as pendamping_nama,
        nullif(btrim(json_payload ->> 'pembukaan/pendamping_peran'), '') as pendamping_peran,

        nullif(btrim(json_payload ->> 'identitas_responden/kunjungan_ke'), '')::integer as kunjungan_ke,
        nullif(btrim(json_payload ->> 'identitas_responden/pengasuh_nama'), '') as pengasuh_nama,
        nullif(btrim(json_payload ->> 'identitas_responden/baduta_nama_lain'), '') as baduta_nama_lain,
        nullif(btrim(json_payload ->> 'identitas_responden/responden_kategori'), '') as responden_kategori,
        nullif(btrim(json_payload ->> 'identitas_responden/responden_kasus_baduta'), '') as responden_kasus_baduta,
        nullif(btrim(json_payload ->> 'rujukan/rujukan_pkm'), '')::integer as rujukan_pkm,
        nullif(btrim(json_payload ->> 'rujukan/rujukan_pmt'), '')::integer as rujukan_pmt,
        nullif(btrim(json_payload ->> 'refleksi/refleksi_masalah'), '') as refleksi_masalah_audio,
        nullif(btrim(json_payload ->> 'refleksi/refleksi_dukungan'), '') as refleksi_dukungan_audio,
        nullif(btrim(json_payload ->> 'refleksi/refleksi_kunjungan'), '') as refleksi_kunjungan_audio,
        nullif(btrim(json_payload ->> 'penutup/upload_dokumentasi'), '') as dokumentasi_file,
        nullif(btrim(json_payload ->> 'penutup/geolokasi'), '') as penutup_geolokasi,
        nullif(btrim(json_payload ->> 'penutup/penutup_durasi'), '')::integer as penutup_durasi,
        json_payload as raw_record
    from source_data
)

select
    typed_data.*,
    ref_prov.label as provinsi,
    ref_kab.label as kota_kabupaten,
    ref_kec.label as kecamatan,
    ref_desa.label as desa_kelurahan,
    ref_pkm.label as puskesmas
from typed_data
left join reference.kobo_list_provinsi_active ref_prov on ref_prov.name = typed_data.provinsi_kode
left join reference.kobo_list_kabupaten_active ref_kab on ref_kab.name = typed_data.kota_kabupaten_kode
left join reference.kobo_list_kecamatan_active ref_kec on ref_kec.name = typed_data.kecamatan_kode
left join reference.kobo_list_desa_active ref_desa on ref_desa.name = typed_data.desa_kelurahan_kode
left join reference.kobo_list_puskesmas_active ref_pkm on ref_pkm.name = typed_data.puskesmas_kode
