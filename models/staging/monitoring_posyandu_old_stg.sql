-- Model: historical Posyandu monitoring scores.
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["monitoring_posyandu_score_stg", "staging", "monitoring_posyandu"]
) }}

select
        nullif(btrim(source::text), '')                        as source,
        nullif(btrim(kunjungan_tanggal::text), '')::date       as kunjungan_tanggal,
        extract(year from kunjungan_tanggal)::int as year,
        extract(year from kunjungan_tanggal)::int || '-Q' || extract(quarter from kunjungan_tanggal)::int as quarter,
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
    from {{ source('raw_sheets', 'monitoring_posyandu_old') }}