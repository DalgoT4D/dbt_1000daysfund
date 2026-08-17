-- Model: Cleans, labels, and deduplicates active Kobo home visits.
{{ config(materialized='table') }}

-- Grain: one row per Kobo submission (latest edit per meta/rootUuid).
-- Codes are resolved to labels from the XLSForm `choices` sheet; the raw codes
-- are kept only inside `typed_data` and are not exposed in the final select.

-- Parse each nonblank raw Kobo payload as JSON.
with source_data as (
    select case when data is null or trim(data::text) = '' then null::jsonb else data::jsonb end as json_payload
    from {{ source('raw_kobo', 'ACTIVEKunjungan_Rumah_Kasus') }}
),

-- Cast raw fields and retain codes needed for label resolution.
typed_data as (
    select
        nullif(json_payload ->> '_id', '')::bigint as submission_id,
        -- internal only: dedup key + tiebreaker, dropped in the final select
        nullif(json_payload ->> 'meta/rootUuid', '') as meta_root_uuid,
        nullif(json_payload ->> '_submission_time', '')::timestamptz as submission_time,

        -- ---------- pembukaan ----------
        nullif(btrim(json_payload ->> 'pembukaan/provinsi'), '') as provinsi_kode,
        nullif(btrim(json_payload ->> 'pembukaan/kota_kabupaten'), '') as kota_kabupaten_kode,
        nullif(btrim(json_payload ->> 'pembukaan/kecamatan'), '') as kecamatan_kode,
        nullif(btrim(json_payload ->> 'pembukaan/desa_kelurahan'), '') as desa_kelurahan_kode,
        nullif(btrim(json_payload ->> 'pembukaan/puskesmas'), '') as puskesmas_kode,
        nullif(btrim(json_payload ->> 'pembukaan/puskesmas_lain'), '') as puskesmas_lain,
        nullif(btrim(json_payload ->> 'pembukaan/posyandu'), '') as posyandu_kode,
        nullif(btrim(json_payload ->> 'pembukaan/posyandu_lain'), '') as posyandu_lain,
        {{ validate_date("(json_payload ->> 'pembukaan/kunjungan_tanggal')") }} as kunjungan_tanggal,
        nullif(json_payload #>> '{_geolocation,0}', '')::numeric as latitude,
        nullif(json_payload #>> '{_geolocation,1}', '')::numeric as longitude,

        nullif(btrim(json_payload ->> 'pembukaan/enumerator_peran'), '') as enumerator_peran_kode,
        -- roster select (relevant only when peran = 'tdf'), free text otherwise
        nullif(btrim(json_payload ->> 'pembukaan/enumerator_nama'), '') as enumerator_nama_kode,
        nullif(btrim(json_payload ->> 'pembukaan/enumerator_nama_lain'), '') as enumerator_nama_lain,
        -- pembukaan/chw_nama is disabled in the form (relevant: peran = 'temp');
        -- add it back here when the kader roster goes live:
        -- nullif(btrim(json_payload ->> 'pembukaan/chw_nama'), '') as chw_nama_kode,
        nullif(btrim(json_payload ->> 'pembukaan/chw_nama_lain'), '') as chw_nama_lain,

        -- select_one opsi_biner: "apakah ada pendamping lain yang hadir?"
        case nullif(btrim(json_payload ->> 'pembukaan/pendamping_lain'), '')
             when '1' then true when '0' then false end as ada_pendamping,
        nullif(btrim(json_payload ->> 'pembukaan/pendamping_peran'), '') as pendamping_peran,  -- select_multiple
        nullif(btrim(json_payload ->> 'pembukaan/pendamping_nama'), '') as pendamping_nama,

        -- ---------- identitas sasaran ----------
        nullif(btrim(json_payload ->> 'identitas_responden/responden_kategori'), '')::integer as responden_kategori_kode,
        nullif(btrim(json_payload ->> 'identitas_responden/responden_group'), '') as responden_group_raw,
        nullif(btrim(json_payload ->> 'identitas_responden/responden_kasus_bumil'), '') as responden_kasus_bumil,
        nullif(btrim(json_payload ->> 'identitas_responden/responden_kasus_baduta'), '') as responden_kasus_baduta,
        nullif(btrim(json_payload ->> 'identitas_responden/kunjungan_ke'), '')::integer as jenis_kunjungan_kode,
        -- ibu_nama / baduta_nama roster selects are also disabled (relevant: kategori = 'temp')
        nullif(btrim(json_payload ->> 'identitas_responden/ibu_nama_lain'), '') as ibu_nama,
        nullif(btrim(json_payload ->> 'identitas_responden/baduta_nama_lain'), '') as baduta_nama,
        nullif(btrim(json_payload ->> 'identitas_responden/pengasuh_nama'), '') as pengasuh_nama,

        -- ---------- refleksi (structured; the three audio questions are relevant: false()) ----------
        nullif(btrim(json_payload ->> 'refleksi/refleksi_masalah_1'), '') as refleksi_masalah,            -- select_multiple opsi_refleksi_1
        nullif(btrim(json_payload ->> 'refleksi/refleksi_masalah_1a'), '') as refleksi_masalah_lain,
        nullif(btrim(json_payload ->> 'refleksi/refleksi_dukungan_1'), '') as refleksi_dukungan,          -- select_multiple opsi_refleksi_2
        nullif(btrim(json_payload ->> 'refleksi/refleksi_dukungan_1a'), '') as refleksi_dukungan_lain,
        nullif(btrim(json_payload ->> 'refleksi/refleksi_kunjungan_1'), '')::integer as kelancaran_skor,  -- 1-3
        nullif(btrim(json_payload ->> 'refleksi/refleksi_kunjungan_2'), '') as refleksi_pendukung,        -- select_multiple opsi_refleksi_3b
        nullif(btrim(json_payload ->> 'refleksi/refleksi_kunjungan_2a'), '') as refleksi_pendukung_lain,
        nullif(btrim(json_payload ->> 'refleksi/refleksi_kunjungan_3'), '') as refleksi_hambatan,         -- select_multiple opsi_refleksi_3c
        nullif(btrim(json_payload ->> 'refleksi/refleksi_kunjungan_3a'), '') as refleksi_hambatan_lain,
        nullif(btrim(json_payload ->> 'refleksi/refleksi_kunjungan_4'), '')::integer as pemahaman_skor,   -- 1-3

        -- ---------- rujukan ----------
        nullif(btrim(json_payload ->> 'rujukan/rujukan_pkm'), '')::integer as rujukan_pkm_kode,
        case nullif(btrim(json_payload ->> 'rujukan/rujukan_pmt'), '')
             when '1' then true when '0' then false end as rujukan_pmt_layak,
        case nullif(btrim(json_payload ->> 'rujukan/rujukan_dapat_pmt'), '')
             when '1' then true when '0' then false end as rujukan_dapat_pmt,

        -- ---------- penutup ----------
        nullif(btrim(json_payload ->> 'penutup/penutup_durasi'), '')::integer as durasi_kode,
        nullif(btrim(json_payload ->> 'penutup/catatan'), '') as catatan,
        nullif(btrim(json_payload ->> 'penutup/upload_dokumentasi'), '') as dokumentasi_file,

        json_payload as raw_record
    from source_data
    where json_payload is not null
),

-- Keep the latest edit for each Kobo submission root.
deduped as (
    -- Kobo edits keep meta/rootUuid stable and issue a new instanceID
    select distinct on (meta_root_uuid) *
    from typed_data
    order by meta_root_uuid, submission_time desc, submission_id desc
)

select
    submission_id,
    kunjungan_tanggal,

    -- geography: labels only; 'lainnya' falls back to the free-text field
    ref_prov.label as provinsi,
    ref_kab.label as kota_kabupaten,
    ref_kec.label as kecamatan,
    ref_desa.label as desa_kelurahan,
    case when d.puskesmas_kode = 'lainnya' then d.puskesmas_lain else ref_pkm.label end as puskesmas,
    case when d.posyandu_kode  = 'lainnya' then d.posyandu_lain  else ref_posy.label end as posyandu,
    d.latitude,
    d.longitude,

    -- enumerator / kader
    case d.enumerator_peran_kode
         when 'chw'    then 'Kader Posyandu'
         when 'satgas' then 'Anggota Satgas'
         when 'tdf'    then 'Staf 1000 Days Fund'
    end as enumerator_peran,
    coalesce(d.enumerator_nama_lain, ref_enum.label, d.enumerator_nama_kode) as enumerator_nama,
    d.chw_nama_lain as chw_nama,
    d.ada_pendamping,
    d.pendamping_peran,
    d.pendamping_nama,

    -- sasaran
    case d.responden_kategori_kode
         when 1 then 'Ibu hamil (trimester 1)'
         when 2 then 'Ibu hamil (trimester 2)'
         when 3 then 'Ibu hamil (trimester 3)'
         when 4 then 'Baduta (0-6 bulan)'
         when 5 then 'Baduta (7-12 bulan)'
         when 6 then 'Baduta (13-24 bulan)'
    end as responden_kategori,
    -- in-form calculate; absent on the older versions, so mirror the form's formula
    coalesce(
        d.responden_group_raw,
        case when d.responden_kategori_kode between 1 and 3 then 'bumil' else 'baduta' end
    ) as responden_group,
    coalesce(d.responden_kasus_bumil, d.responden_kasus_baduta) as responden_kasus,
    coalesce(d.ibu_nama, d.baduta_nama) as responden_nama,
    d.pengasuh_nama,
    case d.jenis_kunjungan_kode
         when 1 then 'Kunjungan pertama'
         when 2 then 'Kunjungan lanjutan'
    end as jenis_kunjungan,

    -- refleksi
    d.refleksi_masalah,
    d.refleksi_masalah_lain,
    d.refleksi_dukungan,
    d.refleksi_dukungan_lain,
    d.refleksi_pendukung,
    d.refleksi_pendukung_lain,
    d.refleksi_hambatan,
    d.refleksi_hambatan_lain,
    d.kelancaran_skor,
    case d.kelancaran_skor
         when 3 then 'Cukup lancar' when 2 then 'Kurang lancar' when 1 then 'Sulit/tidak lancar'
    end as kelancaran,
    d.pemahaman_skor,
    case d.pemahaman_skor
         when 3 then 'Cukup memahami' when 2 then 'Kurang memahami' when 1 then 'Tidak memahami'
    end as pemahaman,
    -- the 340 legacy submissions answered refleksi by voice note; those questions are now
    -- retired, so their refleksi_* columns are null by design
    case when d.refleksi_masalah is null then 'audio' else 'structured' end as refleksi_format,

    -- rujukan
    case d.rujukan_pkm_kode
         when 1 then 'Ya'
         when 0 then 'Tidak'
         when 2 then 'Tidak yakin/disarankan tetapi belum dikonfirmasi'
    end as rujukan_pkm,
    d.rujukan_pmt_layak,
    d.rujukan_dapat_pmt,

    -- penutup
    case d.durasi_kode
         when 1 then '< 10 menit' when 2 then '10-20 menit'
         when 3 then '20-40 menit' when 4 then '40+ menit'
    end as durasi_kunjungan,
    d.durasi_kode as durasi_urutan,
    d.catatan,
    d.dokumentasi_file,

    d.raw_record

from deduped d
left join reference.kobo_list_provinsi_active  ref_prov on ref_prov.name = d.provinsi_kode
left join reference.kobo_list_kabupaten_active ref_kab  on ref_kab.name  = d.kota_kabupaten_kode
left join reference.kobo_list_kecamatan_active ref_kec  on ref_kec.name  = d.kecamatan_kode
left join reference.kobo_list_desa_active      ref_desa on ref_desa.name = d.desa_kelurahan_kode
left join reference.kobo_list_puskesmas_active ref_pkm  on ref_pkm.name  = d.puskesmas_kode
left join reference.kobo_list_posyandu_active  ref_posy on ref_posy.name = d.posyandu_kode
left join reference.tdf_team_jan26             ref_enum on ref_enum.name = d.enumerator_nama_kode
