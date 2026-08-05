{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["score_monitoring_posyandu", "staging", "monitoring_posyandu"]
) }}
{{ config(materialized='table') }}

with active as (

    select
        'active'                         as source,
        kunjungan_tanggal                as kunjungan_tanggal,
        provinsi                         as provinsi,
        kota_kabupaten                   as kota_kabupaten,
        kecamatan                        as kecamatan,
        desa_kelurahan                   as desa_kelurahan,
        puskesmas                        as puskesmas,
        posyandu                         as posyandu,
        persiapan_perc                   as persiapan_perc,
        langkah_1_perc                   as langkah_1_perc,
        langkah_2_perc                   as langkah_2_perc,
        langkah_3_perc                   as langkah_3_perc,
        langkah_4_perc                   as langkah_4_perc,
        langkah_5_perc                   as langkah_5_perc,
        evaluasi_perc                    as evaluasi_perc,
        overall_perc                     as overall_perc
    from {{ ref('active_monitoring_posyandu') }}

),

old as (
    select
        nullif(btrim(source::text), '')                        as source,
        nullif(btrim(kunjungan_tanggal::text), '')::date       as kunjungan_tanggal,
        nullif(btrim(provinsi::text), '')                      as provinsi,
        nullif(btrim(kota_kabupaten::text), '')                as kota_kabupaten,
        nullif(btrim(kecamatan_nama::text), '')                as kecamatan,
        nullif(btrim(desa_kelurahan::text), '')                as desa_kelurahan,
        nullif(btrim(puskesmas::text), '')                     as puskesmas,
        nullif(btrim(posyandu::text), '')                      as posyandu,
        nullif(btrim(persiapan_perc::text), '')::numeric       as persiapan_perc,
        nullif(btrim(langkah_1_perc::text), '')::numeric       as langkah_1_perc,
        nullif(btrim(langkah_2_perc::text), '')::numeric       as langkah_2_perc,
        nullif(btrim(langkah_3_perc::text), '')::numeric       as langkah_3_perc,
        nullif(btrim(langkah_4_perc::text), '')::numeric       as langkah_4_perc,
        nullif(btrim(langkah_5_perc::text), '')::numeric       as langkah_5_perc,
        nullif(btrim(evaluasi_perc::text), '')::numeric        as evaluasi_perc,
        nullif(btrim(overall_perc::text), '')::numeric         as overall_perc
    from {{ source('raw_sheets', 'old_monitoring_posyandu') }}

),

unioned as (

    select * from old
    union all
    select * from active

)

select *
from unioned