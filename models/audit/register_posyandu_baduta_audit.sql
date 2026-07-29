{{ config(materialized='table') }}

with flagged as (

    select
        *,

        (year is null)                                                as fail_year_not_null,
        (year < 2026)                                                  as fail_year_min2026,

        (baduta_usia_bulan is null)                                    as fail_usia_bulan_not_null,
        (baduta_usia_bulan not between 0 and 60)                       as fail_usia_bulan_range,

        (baduta_tinggi_badan is not null
            and baduta_tinggi_badan not between 30 and 125)            as fail_tinggi_badan_range,

        (baduta_berat_badan is not null
            and baduta_berat_badan not between 2 and 25)                as fail_berat_badan_range,

        (baduta_mpasi_biner = 'TRUE'
            and baduta_protein_biner is null)                          as fail_protein_biner_required,

        (baduta_mpasi_biner = 'FALSE'
            and baduta_protein_biner = 'TRUE')                         as fail_protein_biner_false_check,

        (baduta_waz is not null
            and baduta_waz not between -6.0 and 6.0)                    as fail_waz_range,

        (baduta_haz is not null
            and baduta_haz not between -6.0 and 6.0)                    as fail_haz_range

    from {{ ref('register_posyandu_baduta_stg') }}

)

select *
from flagged
where fail_year_not_null
   or fail_year_min2026
   or fail_usia_bulan_not_null
   or fail_usia_bulan_range
   or fail_tinggi_badan_range
   or fail_berat_badan_range
   or fail_protein_biner_required
   or fail_protein_biner_false_check
   or fail_waz_range
   or fail_haz_range