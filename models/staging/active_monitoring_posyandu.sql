-- Updated: 2026-08-05

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

        nullif(btrim(json_payload ->> 'pembukaan/posyandu'), '')                        as posyandu_kode,
        nullif(btrim(json_payload ->> 'pembukaan/provinsi'), '')                        as provinsi_kode,
        nullif(btrim(json_payload ->> 'pembukaan/kota_kabupaten'), '')                  as kota_kabupaten_kode,
        nullif(btrim(json_payload ->> 'pembukaan/kecamatan'), '')                       as kecamatan_kode,
        nullif(btrim(json_payload ->> 'pembukaan/desa_kelurahan'), '')                  as desa_kelurahan_kode,
        nullif(btrim(json_payload ->> 'pembukaan/puskesmas'), '')                       as puskesmas_kode,
        nullif(btrim(json_payload ->> 'pembukaan/enumerator_peran'), '')                as enumerator_peran,
        nullif(btrim(json_payload ->> 'pembukaan/enumerator_nama_lain'), '')            as enumerator_nama_lain,
        {{ validate_date("(json_payload ->> 'pembukaan/kunjungan_tanggal')") }}         as kunjungan_tanggal,

        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_persiapan'), '')::integer     as langkah_persiapan,
        -- Step 1: Registration
        -- Step 1.1: Cadre provides queue number cards.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_1_1'), '')::integer     as langkah_1_1,
        -- Step 1.2: Cadre records participants on the registration sheet.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_1_2'), '')::integer     as langkah_1_2,

        -- Step 2: Measurement
        -- Step 2.1: Cadre prepares height measurement tools.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_2_1'), '')::integer     as langkah_2_1,
        -- Step 2.2: Cadre measures height and weight.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_2_2'), '')::integer     as langkah_2_2,

        -- Step 3: Recording
        -- Step 3.1: Cadre plots the measurements in the KIA book.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_3_1'), '')::integer     as langkah_3_1,
        -- Step 3.2: Cadre fills in the KMS growth chart in the KIA book.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_3_2'), '')::integer     as langkah_3_2,
        -- Step 3.3: Cadre fills in the PB/TB/U chart in the KIA book.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_3_3'), '')::integer     as langkah_3_3,

        -- Step 4: Health Services
        -- Step 4.1: Health workers and cadres work together to serve participants.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_4_1'), '')::integer     as langkah_4_1,

        -- Step 5: Counseling
        -- Step 5.1: Cadre provides counseling to participants.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_5_1'), '')::integer     as langkah_5_1,
        -- Step 5.2: Cadre provides counseling points appropriate to the participant.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_5_2'), '')::integer     as langkah_5_2,
        -- Step 5.3: Cadre confidently provides counseling.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_5_3'), '')::integer     as langkah_5_3,

        -- Evaluation
        -- Evaluation: Conducted at the end of the Posyandu session.
        nullif(btrim(json_payload ->> 'langkah_posyandu/langkah_evaluasi'), '')::integer as langkah_evaluasi,

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
        ref_prov.label                                                                    as provinsi,
        ref_kab.label                                                                     as kota_kabupaten,
        ref_kec.label                                                                     as kecamatan,
        ref_desa.label                                                                    as desa_kelurahan,
        ref_pkm.label                                                                     as puskesmas,
        ref_pyd.label                                                                     as posyandu,
        typed_data.enumerator_peran,
        typed_data.enumerator_nama_lain,
        typed_data.kunjungan_tanggal,


        -- Preparation
        -- Preparation: Conducted before the Posyandu session.
        typed_data.langkah_persiapan                                                            as langkah_persiapan,
        -- Step 1: Registration
        -- Step 1.1: Cadre provides queue number cards.
        typed_data.langkah_1_1                                                                  as langkah_1_1,
        -- Step 1.2: Cadre records participants on the registration sheet.
        typed_data.langkah_1_2                                                                  as langkah_1_2,

        -- Step 2: Measurement
        -- Step 2.1: Cadre prepares height measurement tools.
        typed_data.langkah_2_1                                                                  as langkah_2_1,
        -- Step 2.2: Cadre measures height and weight.
        typed_data.langkah_2_2                                                                  as langkah_2_2,

        -- Step 3: Recording
        -- Step 3.1: Cadre plots the measurements in the KIA book.
        typed_data.langkah_3_1                                                                  as langkah_3_1,
        -- Step 3.2: Cadre fills in the KMS growth chart in the KIA book.
        typed_data.langkah_3_2                                                                  as langkah_3_2,
        -- Step 3.3: Cadre fills in the PB/TB/U chart in the KIA book.
        typed_data.langkah_3_3                                                                  as langkah_3_3,

        -- Step 4: Health Services
        -- Step 4.1: Health workers and cadres work together to serve participants.
        typed_data.langkah_4_1                                                                  as langkah_4_1,

        -- Step 5: Counseling
        -- Step 5.1: Cadre provides counseling to participants.
        typed_data.langkah_5_1                                                                  as langkah_5_1,
        -- Step 5.2: Cadre provides counseling points appropriate to the participant.
        typed_data.langkah_5_2                                                                  as langkah_5_2,
        -- Step 5.3: Cadre confidently provides counseling.
        typed_data.langkah_5_3                                                                  as langkah_5_3,

        -- Evaluation
        -- Evaluation: Conducted at the end of the Posyandu session.
        typed_data.langkah_evaluasi                                                             as langkah_evaluasi,

        typed_data.penutup_catatan

    from typed_data

    left join reference.kobo_list_provinsi_active  ref_prov on ref_prov.name = typed_data.provinsi_kode
    left join reference.kobo_list_kabupaten_active ref_kab  on ref_kab.name  = typed_data.kota_kabupaten_kode
    left join reference.kobo_list_kecamatan_active ref_kec  on ref_kec.name  = typed_data.kecamatan_kode
    left join reference.kobo_list_desa_active      ref_desa on ref_desa.name = typed_data.desa_kelurahan_kode
    left join reference.kobo_list_puskesmas_active ref_pkm  on ref_pkm.name  = typed_data.puskesmas_kode
    left join reference.kobo_list_posyandu_active  ref_pyd  on ref_pyd.name  = typed_data.posyandu_kode

),

scored as (
 
    select
        final.*,
 
        round(
            (coalesce(langkah_persiapan, 0))::numeric
            / nullif((case when langkah_persiapan is not null then 1 else 0 end), 0)
        , 4)                                                                              as persiapan_perc,
 
        round(
            (coalesce(langkah_1_1, 0) + coalesce(langkah_1_2, 0))::numeric
            / nullif((case when langkah_1_1 is not null then 1 else 0 end)
                   + (case when langkah_1_2 is not null then 1 else 0 end), 0)
        , 4)                                                                              as langkah_1_perc,
 
        round(
            (coalesce(langkah_2_1, 0) + coalesce(langkah_2_2, 0))::numeric
            / nullif((case when langkah_2_1 is not null then 1 else 0 end)
                   + (case when langkah_2_2 is not null then 1 else 0 end), 0)
        , 4)                                                                              as langkah_2_perc,
 
        round(
            (coalesce(langkah_3_1, 0) + coalesce(langkah_3_2, 0) + coalesce(langkah_3_3, 0))::numeric
            / nullif((case when langkah_3_1 is not null then 1 else 0 end)
                   + (case when langkah_3_2 is not null then 1 else 0 end)
                   + (case when langkah_3_3 is not null then 1 else 0 end), 0)
        , 4)                                                                              as langkah_3_perc,
 
        round(
            (coalesce(langkah_4_1, 0))::numeric
            / nullif((case when langkah_4_1 is not null then 1 else 0 end), 0)
        , 4)                                                                              as langkah_4_perc,
 
        round(
            (coalesce(langkah_5_1, 0) + coalesce(langkah_5_2, 0) + coalesce(langkah_5_3, 0))::numeric
            / nullif((case when langkah_5_1 is not null then 1 else 0 end)
                   + (case when langkah_5_2 is not null then 1 else 0 end)
                   + (case when langkah_5_3 is not null then 1 else 0 end), 0)
        , 4)                                                                              as langkah_5_perc,
 
        round(
            (coalesce(langkah_evaluasi, 0))::numeric
            / nullif((case when langkah_evaluasi is not null then 1 else 0 end), 0)
        , 4)                                                                              as evaluasi_perc,
 
        round(
            (
                coalesce(langkah_persiapan, 0)
              + coalesce(langkah_1_1, 0) + coalesce(langkah_1_2, 0)
              + coalesce(langkah_2_1, 0) + coalesce(langkah_2_2, 0)
              + coalesce(langkah_3_1, 0) + coalesce(langkah_3_2, 0) + coalesce(langkah_3_3, 0)
              + coalesce(langkah_4_1, 0)
              + coalesce(langkah_5_1, 0) + coalesce(langkah_5_2, 0) + coalesce(langkah_5_3, 0)
                + coalesce(langkah_evaluasi, 0)
            )::numeric
            / nullif(
                (case when langkah_persiapan is not null then 1 else 0 end)
              + (case when langkah_1_1      is not null then 1 else 0 end)
              + (case when langkah_1_2      is not null then 1 else 0 end)
              + (case when langkah_2_1      is not null then 1 else 0 end)
              + (case when langkah_2_2      is not null then 1 else 0 end)
              + (case when langkah_3_1      is not null then 1 else 0 end)
              + (case when langkah_3_2      is not null then 1 else 0 end)
              + (case when langkah_3_3      is not null then 1 else 0 end)
              + (case when langkah_4_1      is not null then 1 else 0 end)
              + (case when langkah_5_1      is not null then 1 else 0 end)
              + (case when langkah_5_2      is not null then 1 else 0 end)
              + (case when langkah_5_3      is not null then 1 else 0 end)
              + (case when langkah_evaluasi is not null then 1 else 0 end)
            , 0)
        , 4)                                                                              as overall_perc
 
    from final
 
)
 
select *
from scored