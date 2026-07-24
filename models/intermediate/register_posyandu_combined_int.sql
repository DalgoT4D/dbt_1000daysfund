{{ config(materialized='table', tags=['intermediate', 'parent', 'register_posyandu']) }}

select
    md5(concat_ws('|',
        {{ profile_name_key('m.unified_name') }},
        {{ profile_name_key('r.kota_kabupaten') }},
        {{ profile_name_key('r.kecamatan') }},
        {{ profile_name_key('r.desa_kelurahan') }}
    )) as parent_id,
    initcap(m.unified_name) as parent_name,
    r.*
from {{ ref('register_posyandu_combined_stg') }} r
join {{ ref('parent_name_fct') }} m
    on m.name_variant = coalesce(r.ibu_nama, r.baduta_pengasuh_nama)
