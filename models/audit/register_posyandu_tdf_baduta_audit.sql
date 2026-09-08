{{ config(materialized='table') }}

with flagged as (

    select
        *,
        (baduta_tanggal_lahir is null or kunjungan_tanggal is null)    as fail_tanggal_null,
        (baduta_tanggal_lahir >= kunjungan_tanggal)                    as fail_tanggal_mismatched,
        (baduta_usia_bulan is not null
            and baduta_usia_bulan not between 0 and 24)                as fail_usia_bulan_range,
        (baduta_berat_badan is null or baduta_tinggi_badan is null)    as fail_bb_tb_null,
        (baduta_waz is not null
            and baduta_waz not between -3.0 and 3.0)                   as fail_waz,
        (baduta_haz is not null
            and baduta_haz not between -3.0 and 3.0)                   as fail_haz
               
    from {{ ref('register_posyandu_tdf_baduta_stg') }}

)

select
    *,

    array_to_string(
        array_remove(
            array[
                case when is_duplicate then 'Duplicate' end,
                case when fail_tanggal_null then 'Tanggal kosong' end,
                case when fail_tanggal_mismatched then 'Tanggal Lahir > Tanggal Kunjungan' end,
                case when fail_usia_bulan_range then 'Usia di luar range' end,
                case when fail_bb_tb_null then 'BB/TB kosong' end,
                case when fail_waz then 'WAZ range' end,
                case when fail_haz then 'HAZ range' end
            ],
            null
        ),
        ', '
    ) as fail_reasons

from flagged
where is_duplicate
    or fail_tanggal_null
    or fail_tanggal_mismatched
    or fail_usia_bulan_range
    or fail_bb_tb_null
    or fail_waz
    or fail_haz