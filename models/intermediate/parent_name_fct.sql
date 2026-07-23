{{ config(materialized='table', tags=['intermediate', 'parent', 'ACTIVEKunjungan_Rumah_Kasus']) }}

with recursive names as (
    select distinct nullif(trim(parent_name), '') as name_variant,
        {{ profile_name_key('parent_name') }} as name_key
    from (
        select baduta_pengasuh_nama as parent_name from {{ ref('register_posyandu_baduta_stg') }}
        union all
        select ibu_nama from {{ ref('register_posyandu_bumil_stg') }}
        union all
        select pengasuh_nama from {{ ref('asesmen_kunjungan_rumah_kasus') }}
    ) source_names
    where nullif(trim(parent_name), '') is not null
),

name_edges as (
    select a.name_key as source_key, b.name_key as target_key
    from names a
    join names b on a.name_key < b.name_key
        and (similarity(a.name_key, b.name_key) >= 0.65
            or a.name_key like b.name_key || ' %'
            or b.name_key like a.name_key || ' %')
),

directed_edges as (
    select source_key, target_key from name_edges
    union all
    select target_key, source_key from name_edges
),

components (root_key, name_key) as (
    select name_key, name_key from names
    union
    select c.root_key, e.target_key
    from components c
    join directed_edges e on e.source_key = c.name_key
),

grouped as (
    select name_key, min(root_key) as group_key
    from components
    group by name_key
),

canonical as (
    select
        g.group_key,
        (array_agg(n.name_variant order by length(n.name_variant) desc, n.name_variant))[1] as unified_name
    from names n
    join grouped g using (name_key)
    group by g.group_key
)

select distinct n.name_variant, c.unified_name
from names n
join grouped g using (name_key)
join canonical c using (group_key)
