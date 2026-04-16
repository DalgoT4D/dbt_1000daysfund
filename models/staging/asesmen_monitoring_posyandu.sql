{{ config(materialized='table') }}

with source_data as (

    select
        case
            when data is null or trim(data::text) = '' then null::jsonb
            else data::jsonb
        end as json_payload
    from {{ source('raw_kobo', 'ACTIVEMonitoring_Posyandu') }}

),

typed_data as (

    select
        nullif(btrim(json_payload ->> '_id'), '')::bigint                               as submission_id,
        case when nullif(btrim(json_payload ->> 'start'), '') is not null then (json_payload ->> 'start')::timestamp end as submission_start_at,
        case when nullif(btrim(json_payload ->> 'end'), '') is not null then (json_payload ->> 'end')::timestamp end as submission_end_at,
        case when nullif(btrim(json_payload ->> '_submission_time'), '') is not null then (json_payload ->> '_submission_time')::timestamp end as submission_time,
        nullif(json_payload #>> '{_geolocation,0}', '')::numeric                        as geolocation_latitude,
        nullif(json_payload #>> '{_geolocation,1}', '')::numeric                        as geolocation_longitude,

        nullif(btrim(json_payload ->> 'pembukaan/posyandu'), '')                        as pembukaan_posyandu,
        nullif(btrim(json_payload ->> 'pembukaan/provinsi'), '')                        as pembukaan_provinsi_kode,
        nullif(btrim(json_payload ->> 'pembukaan/kota_kabupaten'), '')                  as pembukaan_kota_kabupaten_kode,
        nullif(btrim(json_payload ->> 'pembukaan/kecamatan'), '')                       as pembukaan_kecamatan_kode,
        nullif(btrim(json_payload ->> 'pembukaan/desa_kelurahan'), '')                  as pembukaan_desa_kelurahan_kode,
        nullif(btrim(json_payload ->> 'pembukaan/puskesmas'), '')                       as pembukaan_puskesmas_kode,
        nullif(btrim(json_payload ->> 'pembukaan/enumerator_peran'), '')                as pembukaan_enumerator_peran,
        nullif(btrim(json_payload ->> 'pembukaan/enumerator_nama_lain'), '')            as pembukaan_enumerator_nama_lain,
        {{ validate_date("(json_payload ->> 'pembukaan/kunjungan_tanggal')") }}         as pembukaan_kunjungan_tanggal,

        -- Step 1: Registration
        -- Step 1.1: Cadre provides queue number cards.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_1_1'), '')::integer     as kader_menyediakan_kartu_nomor_antrian,
        -- Step 1.2: Cadre records participants on the registration sheet.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_1_2'), '')::integer     as kader_mendata_sasaran_lembar_registrasi,

        -- Step 2: Measurement
        -- Step 2.1: Cadre prepares height measurement tools.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_2_1'), '')::integer     as kader_mempersiapkan_alat_ukur_tinggi_badan,
        -- Step 2.2: Cadre measures height and weight.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_2_2'), '')::integer     as kader_pengukuran_tinggi_badan_berat_badan,

        -- Step 3: Recording
        -- Step 3.1: Cadre plots the measurements in the KIA book.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_3_1'), '')::integer     as kader_penitikan_plotting_buku_kia,
        -- Step 3.2: Cadre fills in the KMS growth chart in the KIA book.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_3_2'), '')::integer     as pengisian_grafik_kms_buku_kia,
        -- Step 3.3: Cadre fills in the PB/TB/U chart in the KIA book.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_3_3'), '')::integer     as pengisian_grafik_pb_tb_u_buku_kia,

        -- Step 4: Health Services
        -- Step 4.1: Health workers and cadres work together to serve participants.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_4_1'), '')::integer     as nakes_kader_bekerja_sama_melayani_sasaran,

        -- Step 5: Counseling
        -- Step 5.1: Cadre provides counseling to participants.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_5_1'), '')::integer     as kader_menyampaikan_penyuluhan_kepada_sasaran,
        -- Step 5.2: Cadre provides counseling points appropriate to the participant.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_5_2'), '')::integer     as kader_menyampaikan_poin_penyuluhan_sesuai_sasaran,
        -- Step 5.3: Cadre confidently provides counseling.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_5_3'), '')::integer     as kader_percaya_diri_memberikan_penyuluhan,

        -- Evaluation
        -- Evaluation: Conducted at the end of the Posyandu session.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_evaluasi'), '')::integer as evaluasi_dilakukan_akhir_posyandu,

        nullif(btrim(json_payload ->> 'penutup/catatan'), '')                           as penutup_catatan,
        json_payload                                                                     as raw_record

    from source_data

),

final as (

    select
        typed_data.submission_id,
        typed_data.submission_start_at,
        typed_data.submission_end_at,
        typed_data.submission_time,
        typed_data.geolocation_latitude,
        typed_data.geolocation_longitude,
        typed_data.pembukaan_posyandu,
        ref_prov.label                                                                    as pembukaan_provinsi,
        ref_kab.label                                                                     as pembukaan_kota_kabupaten,
        ref_kec.label                                                                     as pembukaan_kecamatan,
        ref_desa.label                                                                    as pembukaan_desa_kelurahan,
        ref_pkm.label                                                                     as pembukaan_puskesmas,
        typed_data.pembukaan_enumerator_peran,
        typed_data.pembukaan_enumerator_nama_lain,
        typed_data.pembukaan_kunjungan_tanggal,

        -- Step 1: Registration
        -- Step 1.1: Cadre provides queue number cards.
        typed_data.kader_menyediakan_kartu_nomor_antrian                                  as langkah_1_1_kader_menyediakan_kartu_nomor_antrian,
        -- Step 1.2: Cadre records participants on the registration sheet.
        typed_data.kader_mendata_sasaran_lembar_registrasi                                as langkah_1_2_kader_mendata_sasaran_lembar_registrasi,

        -- Step 2: Measurement
        -- Step 2.1: Cadre prepares height measurement tools.
        typed_data.kader_mempersiapkan_alat_ukur_tinggi_badan                             as langkah_2_1_kader_mempersiapkan_alat_ukur_tinggi_badan,
        -- Step 2.2: Cadre measures height and weight.
        typed_data.kader_pengukuran_tinggi_badan_berat_badan                              as langkah_2_2_kader_pengukuran_tinggi_badan_berat_badan,

        -- Step 3: Recording
        -- Step 3.1: Cadre plots the measurements in the KIA book.
        typed_data.kader_penitikan_plotting_buku_kia                                      as langkah_3_1_kader_penitikan_plotting_buku_kia,
        -- Step 3.2: Cadre fills in the KMS growth chart in the KIA book.
        typed_data.pengisian_grafik_kms_buku_kia                                          as langkah_3_2_pengisian_grafik_kms_buku_kia,
        -- Step 3.3: Cadre fills in the PB/TB/U chart in the KIA book.
        typed_data.pengisian_grafik_pb_tb_u_buku_kia                                      as langkah_3_3_pengisian_grafik_pb_tb_u_buku_kia,

        -- Step 4: Health Services
        -- Step 4.1: Health workers and cadres work together to serve participants.
        typed_data.nakes_kader_bekerja_sama_melayani_sasaran                              as langkah_4_1_nakes_kader_bekerja_sama_melayani_sasaran,

        -- Step 5: Counseling
        -- Step 5.1: Cadre provides counseling to participants.
        typed_data.kader_menyampaikan_penyuluhan_kepada_sasaran                           as langkah_5_1_kader_menyampaikan_penyuluhan_kepada_sasaran,
        -- Step 5.2: Cadre provides counseling points appropriate to the participant.
        typed_data.kader_menyampaikan_poin_penyuluhan_sesuai_sasaran                      as langkah_5_2_kader_menyampaikan_poin_penyuluhan_sesuai_sasaran,
        -- Step 5.3: Cadre confidently provides counseling.
        typed_data.kader_percaya_diri_memberikan_penyuluhan                               as langkah_5_3_kader_percaya_diri_memberikan_penyuluhan,

        -- Evaluation
        -- Evaluation: Conducted at the end of the Posyandu session.
        typed_data.evaluasi_dilakukan_akhir_posyandu                                      as langkah_evaluasi_dilakukan_akhir_posyandu,

        typed_data.penutup_catatan
        
    from typed_data

    left join reference.kobo_list_provinsi_active  ref_prov on ref_prov.name = typed_data.pembukaan_provinsi_kode
    left join reference.kobo_list_kabupaten_active ref_kab  on ref_kab.name  = typed_data.pembukaan_kota_kabupaten_kode
    left join reference.kobo_list_kecamatan_active ref_kec  on ref_kec.name  = typed_data.pembukaan_kecamatan_kode
    left join reference.kobo_list_desa_active      ref_desa on ref_desa.name = typed_data.pembukaan_desa_kelurahan_kode
    left join reference.kobo_list_puskesmas_active ref_pkm  on ref_pkm.name  = typed_data.pembukaan_puskesmas_kode

)

select *
from final
