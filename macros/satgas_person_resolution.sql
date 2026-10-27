{#-
    Within-source person resolution for Satgas home visits.

    Satgas has no date of birth, so the register macro (which leans on DOB for baduta)
    does not fit. Instead, the second anchor is the companion name on the same row:
        baduta: baduta_pengasuh_nama (caregiver)
        bumil : ibu_suami_nama       (husband)

    A "name variant" is one spelling of a person's name in one place:
        responden_kategori + kabupaten + name + desa
    Variants judged to be the same person are chained into one group; each group gets
    one src_person_id and one display unified_name.

    Same-person rules (always within the same kabupaten and responden_kategori):
        S1 same desa and name similarity >= 0.6                 (name typo)
        S2 same name and desa similarity >= 0.6                 (desa typo)
        S3 name similarity >= 0.5 and companion name
           similarity >= 0.6                                    (bigger name change, or
                                                                 desa written differently)
        S4 same desa, name similarity >= 0.4 and companion
           similarity >= 0.4                                    (abbreviated name plus a
                                                                 differently written
                                                                 parent list, e.g.
                                                                 'mindiani/mahidi' vs
                                                                 'mahidi /windiana')
    Sibling guard: two variants are never merged if both were visited on the same date
    in the same desa (two children of one household visited together).

    Usage inside a model whose WITH chain starts with `with recursive`:
        {{ satgas_person_ctes('my_rows', 'satgas') }}
        select r.*, m.src_person_id, m.unified_name
        from my_rows r
        left join sp_person_map m on m.sp_variant_id = {{ satgas_variant_id('r') }}

    Input must carry: responden_kategori, kota_kabupaten, desa_kelurahan,
    kunjungan_tanggal, ibu_nama, ibu_suami_nama, baduta_nama, baduta_pengasuh_nama.
    Requires pg_trgm.
-#}

{% macro satgas_variant_id(alias) -%}
    case
        when {{ profile_name_key('coalesce(' ~ alias ~ '.baduta_nama, ' ~ alias ~ '.ibu_nama)') }} is not null
        then concat_ws('|',
            {{ alias }}.responden_kategori,
            coalesce({{ profile_name_key(alias ~ '.kota_kabupaten') }}, ''),
            {{ profile_name_key('coalesce(' ~ alias ~ '.baduta_nama, ' ~ alias ~ '.ibu_nama)') }},
            coalesce({{ profile_name_key(alias ~ '.desa_kelurahan') }}, 'no_desa'))
    end
{%- endmacro %}


{% macro satgas_person_ctes(input_cte, src) -%}

-- Matching keys for every input row.
sp_rows as (
    select
        {{ satgas_variant_id('i') }}                                                as sp_variant_id,
        i.responden_kategori,
        coalesce({{ profile_name_key('i.kota_kabupaten') }}, '')                    as kab_key,
        {{ profile_name_key('i.desa_kelurahan') }}                                  as desa_key,
        {{ profile_name_key('coalesce(i.baduta_nama, i.ibu_nama)') }}               as name_key,
        {{ profile_name_key('coalesce(i.baduta_pengasuh_nama, i.ibu_suami_nama)') }} as companion_key,
        coalesce(i.baduta_nama, i.ibu_nama)                                         as raw_name,
        i.kunjungan_tanggal
    from {{ input_cte }} i
),

-- One row per name variant.
sp_variants as (
    select
        sp_variant_id,
        responden_kategori,
        kab_key,
        name_key,
        max(desa_key)                                        as desa_key,
        mode() within group (order by companion_key)         as companion_key
    from sp_rows
    where sp_variant_id is not null
    group by 1, 2, 3, 4
),

-- Visit days per variant (for the sibling guard).
sp_sessions as (
    select distinct sp_variant_id, desa_key, kunjungan_tanggal
    from sp_rows
    where sp_variant_id is not null
      and kunjungan_tanggal is not null
),

-- Variant pairs judged to be the same person.
sp_edges as (
    select a.sp_variant_id as a_id, b.sp_variant_id as b_id
    from sp_variants a
    join sp_variants b
        on  a.responden_kategori = b.responden_kategori
        and a.kab_key = b.kab_key
        and a.sp_variant_id < b.sp_variant_id
        and (a.name_key = b.name_key or similarity(a.name_key, b.name_key) >= 0.4)
    where
        (    (a.desa_key = b.desa_key and similarity(a.name_key, b.name_key) >= 0.6)
          or (a.name_key = b.name_key and similarity(a.desa_key, b.desa_key) >= 0.6)
          or (similarity(a.name_key, b.name_key) >= 0.5
              and similarity(a.companion_key, b.companion_key) >= 0.6)
          or (a.desa_key = b.desa_key
              and similarity(a.name_key, b.name_key) >= 0.4
              and similarity(a.companion_key, b.companion_key) >= 0.4) )
        and not exists (
            select 1
            from sp_sessions sa
            join sp_sessions sb
                on  sa.kunjungan_tanggal = sb.kunjungan_tanggal
                and sa.desa_key is not distinct from sb.desa_key
            where sa.sp_variant_id = a.sp_variant_id
              and sb.sp_variant_id = b.sp_variant_id
        )
),

-- Every variant reachable from every variant through sp_edges (connected components).
sp_reach (variant_id, member_id) as (
    select sp_variant_id, sp_variant_id from sp_variants
    union
    select r.variant_id,
           case when e.a_id = r.member_id then e.b_id else e.a_id end
    from sp_reach r
    join sp_edges e on r.member_id in (e.a_id, e.b_id)
),

-- Group root = smallest variant id in the component.
sp_groups as (
    select variant_id, min(member_id) as root
    from sp_reach
    group by 1
),

-- Display name per group: most frequent spelling, then longest, then alphabetical.
sp_canonical as (
    select distinct on (g.root)
        g.root,
        r.raw_name as unified_name
    from sp_rows r
    join sp_groups g on g.variant_id = r.sp_variant_id
    group by g.root, r.raw_name
    order by g.root, count(*) desc, length(r.raw_name) desc, r.raw_name
),

-- Variant -> person.
sp_person_map as (
    select
        g.variant_id                              as sp_variant_id,
        md5('{{ src }}|' || g.root)               as src_person_id,
        c.unified_name
    from sp_groups g
    join sp_canonical c using (root)
)

{%- endmacro %}