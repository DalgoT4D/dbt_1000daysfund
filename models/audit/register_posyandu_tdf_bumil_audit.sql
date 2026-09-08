{{ config(materialized='table') }}

with flagged as (

    select
        *,
        (kunjungan_tanggal is null or ibu_hpht is null)                as fail_tanggal_null,
        (kunjungan_tanggal >= ibu_hpht)                                as fail_tanggal_mismatched,
        (ibu_usia_kehamilan_minggu not between 0 and 40)               as fail_usia_kehamilan_range,
        (ibu_tekanan_darah_sistolik is null or ibu_tekanan_darah_diastolik is null)    as fail_tekanan_darah_null,
        (ibu_tekanan_darah_sistolik is not null
            and ibu_tekanan_darah_sistolik not between 90 and 120)                   as fail_sistolik,
        (ibu_tekanan_darah_diastolik is not null
            and ibu_tekanan_darah_diastolik not between 60 and 80)                   as fail_diastolik,
        (ibu_lila is null)                                             as fail_lila_null,
        (ibu_lila is null is not null
            and ibu_lila not between 20 and 30)                as fail_lila_range  
    from {{ ref('register_posyandu_tdf_bumil_stg') }}

)

select
    *,

    array_to_string(
        array_remove(
            array[
                case when is_duplicate then 'Duplicate' end,
                case when fail_tanggal_null then 'Tanggal kosong' end,
                case when fail_tanggal_mismatched then 'Tanggal Kunjungan > HPHT' end,
                case when fail_usia_kehamilan_range then 'Usia kehamilan di luar range' end,
                case when fail_tekanan_darah_null then 'Tekanan darah kosong' end,
                case when fail_sistolik then 'Range sistolik' end,
                case when fail_diastolik then 'Range diastolik' end,
                case when fail_lila_null then 'Lila kosong' end,
                case when fail_lila_range then 'Lila di luar range' end
            ],
            null
        ),
        ', '
    ) as fail_reasons

from flagged
where is_duplicate
    or fail_tanggal_null
    or fail_tanggal_mismatched
    or fail_usia_kehamilan_range
    or fail_tekanan_darah_null
    or fail_sistolik
    or fail_diastolik
    or fail_lila_null
    or fail_lila_range