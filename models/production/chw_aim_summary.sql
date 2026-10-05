{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["kobo", "chw_aim", "production"]
) }}

with ranked as (
    select
        *,
        row_number() over (
            partition by
                kota_kabupaten,
                kecamatan,
                desa_kelurahan,
                case when puskesmas = 'Lainnya' then puskesmas_lain else puskesmas end
            order by kunjungan_tanggal desc, submission_time desc
        ) as rn
    from {{ ref('chw_aim_active_stg') }}
),

latest as (
    select
        *,
        (
            komponen_asesmen_peran_dan_rekrutmen
          + komponen_asesmen_pelatihan
          + komponen_asesmen_akreditasi
          + komponen_asesmen_peralatan_dan_perlengkapan
          + komponen_asesmen_supervisi
          + komponen_asesmen_insentif
          + komponen_asesmen_dukungan_komunitas
          + komponen_asesmen_peluang_untuk_maju
          + komponen_asesmen_data
          + komponen_asesmen_sistem_kesehatan
        ) / 10.0 as row_overall
    from ranked
    where rn = 1
)

select
    kota_kabupaten,
    count(*)                                                        as n_assessments,
    round(avg(komponen_asesmen_peran_dan_rekrutmen), 2)             as peran_dan_rekrutmen,
    round(avg(komponen_asesmen_pelatihan), 2)                       as pelatihan,
    round(avg(komponen_asesmen_akreditasi), 2)                      as akreditasi,
    round(avg(komponen_asesmen_peralatan_dan_perlengkapan), 2)      as peralatan_dan_perlengkapan,
    round(avg(komponen_asesmen_supervisi), 2)                       as supervisi,
    round(avg(komponen_asesmen_insentif), 2)                        as insentif,
    round(avg(komponen_asesmen_dukungan_komunitas), 2)              as dukungan_komunitas,
    round(avg(komponen_asesmen_peluang_untuk_maju), 2)              as peluang_untuk_maju,
    round(avg(komponen_asesmen_data), 2)                            as data,
    round(avg(komponen_asesmen_sistem_kesehatan), 2)                as sistem_kesehatan,
    round(avg(row_overall), 2)                                      as overall_score,
    case
        when avg(row_overall) < 2 then '🔴 LOW'
        when avg(row_overall) < 3 then '🟡 MEDIUM'
        else '🟢 HIGH'
    end                                                             as status
from latest
group by kota_kabupaten
order by kota_kabupaten