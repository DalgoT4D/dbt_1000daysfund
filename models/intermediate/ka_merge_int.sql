{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["ka"]
) }}

with recursive current_modules_raw as (
    select
        email,
        name,
        clean_name,
        role,
        whatsapp,
        district,
        province,
        puskesmas,
        village,
        cast(year as integer) as year,
        quarter,
        date,
        timestamp_raw,
        score,
        'modul_1' as modul
    from {{ ref('ka_modul_1_clean') }}

    union all

    select
        email,
        name,
        clean_name,
        role,
        whatsapp,
        district,
        province,
        puskesmas,
        village,
        cast(year as integer) as year,
        quarter,
        date,
        timestamp_raw,
        score,
        'modul_2' as modul
    from {{ ref('ka_modul_2_clean') }}

    union all

    select
        email,
        name,
        clean_name,
        role,
        whatsapp,
        district,
        province,
        puskesmas,
        village,
        cast(year as integer) as year,
        quarter,
        date,
        timestamp_raw,
        score,
        'modul_3' as modul
    from {{ ref('ka_modul_3_clean') }}
),

distinct_names as (
    select distinct clean_name, name, district, email, whatsapp
    from current_modules_raw
    where clean_name is not null
),

normalized_names as (
    select
        clean_name,
        name,
        district,
        email,
        whatsapp,
        trim(
            regexp_replace(
                regexp_replace(clean_name, '[^a-z ]', '', 'g'),
                '\s+',
                ' ',
                'g'
            )
        ) as name_plain
    from distinct_names
),

name_pairs as (
    select
        a.clean_name,
        least(a.clean_name, b.clean_name) as root
    from normalized_names a
    join normalized_names b
        on a.clean_name <> b.clean_name
        and a.name_plain <> ''
        and b.name_plain <> ''
        and (
            a.district = b.district
            or a.email = b.email
            or a.whatsapp = b.whatsapp
        )
        -- Catch close variants plus short-prefix forms like "Adea" vs "Adea O".
        and (
            similarity(a.name_plain, b.name_plain) >= 0.64
            or a.name_plain like b.name_plain || ' %'
            or b.name_plain like a.name_plain || ' %'
        )
),

name_groups (clean_name, root) as (
    select clean_name, root from name_pairs
    union
    select np.clean_name, ng.root
    from name_pairs np
    join name_groups ng on np.root = ng.clean_name
),

name_groups_final as (
    select clean_name, min(root) as name_group_key
    from (
        select clean_name, root from name_groups
        union all
        select ng.clean_name, ng2.root
        from name_groups ng
        join name_groups ng2 on ng.root = ng2.clean_name
    ) chained
    group by clean_name

    union all

    select dn.clean_name, dn.clean_name
    from distinct_names dn
    where not exists (
        select 1
        from name_groups ng
        where ng.clean_name = dn.clean_name
    )
),

with_unified as (
    select
        cm.*,
        first_value(cm.name) over (
            partition by coalesce(
                ngf.name_group_key,
                cm.clean_name,
                lower(trim(cm.name)),
                cm.email,
                cm.whatsapp
            )
            order by length(coalesce(cm.clean_name, '')) desc, cm.timestamp_raw asc
            rows between unbounded preceding and unbounded following
        ) as unified_name
    from current_modules_raw cm
    left join name_groups_final ngf on cm.clean_name = ngf.clean_name
),

ranked as (
    select
        *,
        row_number() over (
            partition by
                modul,
                quarter,
                coalesce(email, whatsapp),
                district,
                coalesce(unified_name, name, clean_name)
            order by timestamp_raw desc
        ) as rn
    from with_unified
)

select
    email,
    name,
    clean_name,
    unified_name,
    role,
    whatsapp,
    district,
    province,
    puskesmas,
    village,
    year,
    quarter,
    date,
    timestamp_raw,
    score,
    modul,
    case when rn = 1 then true else false end as is_latest_score
from ranked
