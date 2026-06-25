{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["training_data_stg", "staging", "ka"]
) }}

with recursive source as (
    select * from {{ source('raw_sheets', 'ka_modul_01') }}
),

district_typos as (
    select typo, district from {{ ref('district_typos') }}
),

parsed as (
    select
        cast("Timestamp" as timestamp) as timestamp_raw,
        cast(cast("Timestamp" as timestamp) as date) as date,
        extract(year from cast("Timestamp" as timestamp)) as year,
        concat(cast(extract(year from cast("Timestamp" as timestamp)) as varchar),
                '-Q',
                cast(extract(quarter from cast("Timestamp" as timestamp)) as varchar)
            ) as quarter,
        "Nama" as name,
        {{ normalize_unicode('"Nama"') }} as clean_name,
        lower(trim("Email_Address")) as email,
        trim("Peran_Anda") as role_raw,
        "Nomor_HP_WA" as whatsapp,
        {{ normalize_unicode('"Kabupaten_Kota"') }} as district_raw,
        trim("Provinsi") as province,
        trim("Puskesmas") as puskesmas,
        trim("Desa_Kelurahan") as village,
        round(
            (
                cast(trim(split_part("Score", '/', 1)) as numeric)
                / nullif(cast(trim(split_part("Score", '/', 2)) as numeric), 0)
            ) * 100,2) as score
    from source
),

filtered_quarters as (
    select *
    from parsed
    -- where quarter in ('2026-Q1', '2026-Q2')
    where date >= date '2026-04-01'
),


district_corrected as (
    select
        p.*,
        coalesce(nullif(dt.district, ''), p.district_raw) as district,
        case
            when lower(coalesce(p.role_raw, '')) = 'nakes' then 'Health Worker'
            when lower(coalesce(p.role_raw, '')) ~ 'bidan' then 'Health Worker'
            when lower(coalesce(p.role_raw, '')) = 'kader posyandu' then 'Community Health Worker'
            when lower(coalesce(p.role_raw, '')) ~ 'tim pendamping keluarga' then 'Community Health Worker'
            when lower(coalesce(p.role_raw, '')) ~ 'mahasiswa' then 'Student'
            when lower(coalesce(p.role_raw, '')) ~ 'mahasiswi' then 'Student'
            when lower(coalesce(p.role_raw, '')) ~ 'novo' then 'Student'
            else 'Public'
        end as role
    from filtered_quarters p
    left join district_typos dt on p.district_raw = lower(trim(dt.typo))
),
distinct_names as (
    select distinct clean_name, name, district, email, whatsapp
    from district_corrected
),
-- Step 1: direct pairs that are similar AND share at least one anchor
name_pairs as (
    select
        a.clean_name,
        least(a.clean_name, b.clean_name) as root
    from distinct_names a
    join distinct_names b
        on similarity(
            regexp_replace(a.clean_name, '[^a-z ]', '', 'g'),
            regexp_replace(b.clean_name, '[^a-z ]', '', 'g')
        ) >= 0.64
        and a.clean_name <> b.clean_name
        and (
            a.district  = b.district    -- same area
            or a.email    = b.email     -- or same email
            or a.whatsapp = b.whatsapp  -- or same phone
        )
),

-- Step 2: recursive transitive closure — chains A→B→C into one group
name_groups (clean_name, root) as (
    select clean_name, root from name_pairs
    union
    select np.clean_name, ng.root
    from name_pairs np
    join name_groups ng on np.root = ng.clean_name
),


-- Step 3: pick lowest root per name; unmatched names are their own group
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

    select clean_name, clean_name
    from distinct_names
    where clean_name not in (select clean_name from name_groups)
),

-- Step 4: assign unified_name as longest name in the group
unified as (
    select
        b.*,
        first_value(b.name) over (
            partition by ngf.name_group_key
            order by length(b.clean_name) desc, b.timestamp_raw asc
            rows between unbounded preceding and unbounded following
        ) as unified_name
    from district_corrected b
    left join name_groups_final ngf on b.clean_name = ngf.clean_name
),

-- from unified
ranked as (
    select
        *,
        row_number() over (
            partition by quarter, email, unified_name
            order by timestamp_raw desc
        ) as rn
    from unified
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
    score
from ranked
where rn = 1
