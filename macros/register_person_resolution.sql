{#-
    Within-source person resolution for the Posyandu registers.

    A "name variant" is one spelling of a person's name in one place:
        baduta: responden_kategori + kabupaten + name + DOB
        bumil : responden_kategori + kabupaten + name + desa
    Variants that are judged to be the same person are chained into one group, and
    each group becomes one src_person_id with one display unified_name.

    Same-person rules (always within the same kabupaten and responden_kategori):
        baduta  same DOB and name similarity >= 0.5
                same DOB and abbreviated name: similarity >= 0.3 + same last name +
                    same first initial, or similarity >= 0.4 + same first name
                name similarity >= 0.8 and DOB within 31 days (DOB typo) and same
                    desa or posyandu
        bumil   same desa and name similarity >= 0.6
                same name and desa similarity >= 0.6 (desa spelling variant)
    Twin/sibling guard: two variants are never merged if both were seen at the same
    posyandu on the same visit date, since one person is not registered twice in one
    session.

    Usage inside a model whose WITH chain starts with `with recursive`:

        with recursive
        ...,
        my_rows as (...),
        {{ register_person_ctes('my_rows', 'tdf') }}
        select r.*, m.src_person_id, m.unified_name
        from my_rows r
        left join rp_person_map m
            on m.rp_variant_id = {{ register_variant_id('r') }}

    The input must carry: responden_kategori, kota_kabupaten, desa_kelurahan,
    posyandu, kunjungan_tanggal, baduta_nama, ibu_nama, baduta_tanggal_lahir.
    Requires pg_trgm.
-#}

{% macro register_variant_id(alias) -%}
    case
        when {{ profile_name_key('coalesce(' ~ alias ~ '.baduta_nama, ' ~ alias ~ '.ibu_nama)') }} is not null
        then concat_ws('|',
            {{ alias }}.responden_kategori,
            coalesce({{ profile_name_key(alias ~ '.kota_kabupaten') }}, ''),
            {{ profile_name_key('coalesce(' ~ alias ~ '.baduta_nama, ' ~ alias ~ '.ibu_nama)') }},
            case
                when {{ alias }}.responden_kategori = 'baduta'
                    then coalesce({{ alias }}.baduta_tanggal_lahir::text, 'no_dob')
                else coalesce({{ profile_name_key(alias ~ '.desa_kelurahan') }}, 'no_desa')
            end)
    end
{%- endmacro %}


{% macro register_person_ctes(input_cte, src) -%}

-- Matching keys for every input row.
rp_rows as (
    select
        {{ register_variant_id('i') }}                                    as rp_variant_id,
        i.responden_kategori,
        coalesce({{ profile_name_key('i.kota_kabupaten') }}, '')          as kab_key,
        {{ profile_name_key('i.desa_kelurahan') }}                        as desa_key,
        {{ profile_name_key('i.posyandu') }}                              as posyandu_key,
        {{ profile_name_key('coalesce(i.baduta_nama, i.ibu_nama)') }}     as name_key,
        coalesce(i.baduta_nama, i.ibu_nama)                               as raw_name,
        i.baduta_tanggal_lahir                                            as dob,
        i.kunjungan_tanggal
    from {{ input_cte }} i
),

-- One row per name variant.
rp_variants as (
    select
        rp_variant_id,
        responden_kategori,
        kab_key,
        name_key,
        split_part(name_key, ' ', 1)                         as first_token,
        regexp_replace(name_key, '^.* ', '')                 as last_token,
        max(dob)                                             as dob,
        mode() within group (order by desa_key)              as desa_key,
        mode() within group (order by posyandu_key)          as posyandu_key
    from rp_rows
    where rp_variant_id is not null
    group by 1, 2, 3, 4
),

-- Posyandu sessions each variant was seen at (for the twin guard).
rp_sessions as (
    select distinct rp_variant_id, posyandu_key, kunjungan_tanggal
    from rp_rows
    where rp_variant_id is not null
      and posyandu_key is not null
      and kunjungan_tanggal is not null
),

-- Variant pairs judged to be the same person.
rp_edges as (
    select a.rp_variant_id as a_id, b.rp_variant_id as b_id
    from rp_variants a
    join rp_variants b
        on  a.responden_kategori = b.responden_kategori
        and a.kab_key = b.kab_key
        and a.rp_variant_id < b.rp_variant_id
        and (a.dob = b.dob
             or a.name_key = b.name_key
             or similarity(a.name_key, b.name_key) >= 0.3)
    where
        case
            when a.responden_kategori = 'baduta' then
                (a.dob = b.dob and similarity(a.name_key, b.name_key) >= 0.5)
                or (a.dob = b.dob
                    and (   (similarity(a.name_key, b.name_key) >= 0.3
                             and a.last_token = b.last_token
                             and left(a.first_token, 1) = left(b.first_token, 1))
                         or (similarity(a.name_key, b.name_key) >= 0.4
                             and a.first_token = b.first_token)))
                or (similarity(a.name_key, b.name_key) >= 0.8
                    and abs(a.dob - b.dob) <= 31
                    and (a.desa_key = b.desa_key or a.posyandu_key = b.posyandu_key))
            else
                (a.desa_key = b.desa_key and similarity(a.name_key, b.name_key) >= 0.6)
                or (a.name_key = b.name_key and similarity(a.desa_key, b.desa_key) >= 0.6)
        end
        and not exists (
            select 1
            from rp_sessions sa
            join rp_sessions sb
                on  sa.posyandu_key = sb.posyandu_key
                and sa.kunjungan_tanggal = sb.kunjungan_tanggal
            where sa.rp_variant_id = a.rp_variant_id
              and sb.rp_variant_id = b.rp_variant_id
        )
),

-- Every variant reachable from every variant through rp_edges (connected components).
rp_reach (variant_id, member_id) as (
    select rp_variant_id, rp_variant_id from rp_variants
    union
    select r.variant_id,
           case when e.a_id = r.member_id then e.b_id else e.a_id end
    from rp_reach r
    join rp_edges e on r.member_id in (e.a_id, e.b_id)
),

-- Group root = smallest variant id in the component.
rp_groups as (
    select variant_id, min(member_id) as root
    from rp_reach
    group by 1
),

-- Display name per group: most frequent spelling, then longest, then alphabetical.
rp_canonical as (
    select distinct on (g.root)
        g.root,
        r.raw_name as unified_name
    from rp_rows r
    join rp_groups g on g.variant_id = r.rp_variant_id
    group by g.root, r.raw_name
    order by g.root, count(*) desc, length(r.raw_name) desc, r.raw_name
),

-- Variant -> person.
rp_person_map as (
    select
        g.variant_id                              as rp_variant_id,
        md5('{{ src }}|' || g.root)               as src_person_id,
        c.unified_name
    from rp_groups g
    join rp_canonical c using (root)
)

{%- endmacro %}