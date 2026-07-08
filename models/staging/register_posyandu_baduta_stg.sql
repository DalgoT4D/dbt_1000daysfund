{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["register_posyandu_baduta_zscore", "marts"]
) }}

with baduta as (

    select * from {{ ref('stg_register_posyandu_baduta') }}

),

ref_wfa as (

    select
        sex,
        month::int    as month,
        l::numeric    as l,
        m::numeric    as m,
        s::numeric    as s
    from {{ ref('who_wfa') }}

),

ref_lhfa as (

    select
        sex,
        month::int    as month,
        l::numeric    as l,
        m::numeric    as m,
        s::numeric    as s
    from {{ ref('who_lhfa') }}

),

zscored as (

    select
        b.*,

        -- Weight-for-age
        round(
            ({{ who_lms_zscore('b.baduta_berat_badan', 'w.l', 'w.m', 'w.s') }})::numeric,
            2
        ) as z_wfa,

        -- Height/Length-for-age
        round(
            ({{ who_lms_zscore('b.baduta_tinggi_badan', 'h.l', 'h.m', 'h.s') }})::numeric,
            2
        ) as z_hfa

    from baduta b

    -- WHO reference sex encoding: Laki-laki -> M, Perempuan -> F
    left join ref_wfa w
        on  w.month = b.baduta_usia_bulan
        and w.sex   = case b.baduta_gender
                          when 'Laki-laki' then 'M'
                          when 'Perempuan' then 'F'
                      end

    left join ref_lhfa h
        on  h.month = b.baduta_usia_bulan
        and h.sex   = case b.baduta_gender
                          when 'Laki-laki' then 'M'
                          when 'Perempuan' then 'F'
                      end

)

select * from zscored