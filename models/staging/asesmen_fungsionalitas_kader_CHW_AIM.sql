{{ config(materialized='table') }}

with source_data as (

    select
        case when data is null or trim(data::text) = '' then null::jsonb else data::jsonb end as json_payload
    from {{ source("raw_kobo", "ACTIVEAsesmen_Fungsionalitas_Kader_CHW_AIM") }}

),

final as (

    select
        nullif(json_payload ->> '_id', '')::bigint                                          as submission_id,
        case when nullif(json_payload ->> '_submission_time', '') is not null then (json_payload ->> '_submission_time')::timestamp end as submission_time,
        nullif(json_payload ->> '_status', '')                                              as submission_status,

        ref_prov.label                                                                      as pembukaan_provinsi,
        ref_kab.label                                                                       as pembukaan_kota_kabupaten,
        ref_kec.label                                                                       as pembukaan_kecamatan,
        ref_desa.label                                                                      as pembukaan_desa_kelurahan,
        ref_pkm.label                                                                       as pembukaan_puskesmas,
        case when lower(coalesce(nullif(json_payload ->> 'pembukaan/puskesmas', ''), '')) = 'lainnya' then nullif(json_payload ->> 'pembukaan/puskesmas_lain', '') end as pembukaan_puskesmas_lain,
        ref_enum.label                                                                      as pembukaan_enumerator,
        {{ validate_date("(json_payload ->> 'pembukaan/kunjungan_tanggal')") }}             as pembukaan_kunjungan_tanggal,
        nullif(json_payload ->> 'komponen_asesmen/komponen_1', '')::integer                 as komponen_asesmen_peran_dan_rekrutmen,
        nullif(json_payload ->> 'komponen_asesmen/komponen_2', '')::integer                 as komponen_asesmen_pelatihan,
        nullif(json_payload ->> 'komponen_asesmen/komponen_3', '')::integer                 as komponen_asesmen_akreditasi,
        nullif(json_payload ->> 'komponen_asesmen/komponen_4', '')::integer                 as komponen_asesmen_peralatan_dan_perlengkapan,
        nullif(json_payload ->> 'komponen_asesmen/komponen_5', '')::integer                 as komponen_asesmen_supervisi,
        nullif(json_payload ->> 'komponen_asesmen/komponen_6', '')::integer                 as komponen_asesmen_insentif,
        nullif(json_payload ->> 'komponen_asesmen/komponen_7', '')::integer                 as komponen_asesmen_dukungan_komunitas,
        nullif(json_payload ->> 'komponen_asesmen/komponen_8', '')::integer                 as komponen_asesmen_peluang_untuk_maju,
        nullif(json_payload ->> 'komponen_asesmen/komponen_9', '')::integer                 as komponen_asesmen_data,
        nullif(json_payload ->> 'komponen_asesmen/komponen_10', '')::integer                as komponen_asesmen_sistem_kesehatan

    from source_data

    left join reference.kobo_list_provinsi_active  ref_prov  on ref_prov.name  = nullif(json_payload ->> 'pembukaan/provinsi', '')
    left join reference.kobo_list_kabupaten_active ref_kab   on ref_kab.name   = nullif(json_payload ->> 'pembukaan/kota_kabupaten', '')
    left join reference.kobo_list_kecamatan_active ref_kec   on ref_kec.name   = nullif(json_payload ->> 'pembukaan/kecamatan', '')
    left join reference.kobo_list_desa_active      ref_desa  on ref_desa.name  = nullif(json_payload ->> 'pembukaan/desa_kelurahan', '')
    left join reference.kobo_list_puskesmas_active ref_pkm   on ref_pkm.name   = nullif(json_payload ->> 'pembukaan/puskesmas', '')
    left join reference.tdf_team_jan26             ref_enum  on ref_enum.name  = nullif(json_payload ->> 'pembukaan/enumerator_nama', '')

)

select *
from final