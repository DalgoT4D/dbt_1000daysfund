{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["intermediate", "training_12", "training"]
) }}

with base as (
    select
        *,
        coalesce(unified_name_key, nama_key) as person_name_key,
        case when coalesce(unified_name_key, nama_key) is not null and desa_key is not null and puskesmas_key is not null then concat_ws('|', coalesce(unified_name_key, nama_key), desa_key, puskesmas_key) end as person_name_desa_pusk_key,
        case when coalesce(unified_name_key, nama_key) is not null and puskesmas_key is not null then concat_ws('|', coalesce(unified_name_key, nama_key), puskesmas_key) end as person_name_pusk_key
    from {{ ref('training_12_forms_stg') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by form_tag, coalesce(
                case when phone_key is not null then concat('ph:', phone_key) end,
                case when nik_key is not null then concat('nik:', nik_key) end,
                case when person_name_desa_pusk_key is not null then concat('ndp:', person_name_desa_pusk_key) end,
                case when person_name_pusk_key is not null then concat('np:', person_name_pusk_key) end,
                case when person_name_key is not null then concat('n:', person_name_key) end,
                concat('row:', record_id)
            )
            order by score desc nulls last, timestamp_raw desc nulls last, record_id
        ) as dedupe_rn
    from base
),

dedup as (
    select * from ranked where dedupe_rn = 1
),

pre_dedup as (
    select * from dedup where form_tag = 'pre'
),

post_dedup as (
    select * from dedup where form_tag = 'post'
),

pre_t1 as (
    select *, row_number() over (partition by phone_key order by timestamp_raw, record_id) as key_rn
    from pre_dedup
    where phone_key is not null
),

post_t1 as (
    select *, row_number() over (partition by phone_key order by timestamp_raw, record_id) as key_rn
    from post_dedup
    where phone_key is not null
),

match_t1 as (
    select p.record_id as pre_record_id, q.record_id as post_record_id, 'T1_phone'::text as match_tier, 100::integer as match_score
    from pre_t1 p
    join post_t1 q on p.phone_key = q.phone_key and p.key_rn = q.key_rn
),

pre_after_t1 as (
    select p.* from pre_dedup p left join match_t1 m on p.record_id = m.pre_record_id where m.pre_record_id is null
),

post_after_t1 as (
    select q.* from post_dedup q left join match_t1 m on q.record_id = m.post_record_id where m.post_record_id is null
),

pre_t2 as (
    select *, row_number() over (partition by nik_key order by timestamp_raw, record_id) as key_rn
    from pre_after_t1
    where nik_key is not null
),

post_t2 as (
    select *, row_number() over (partition by nik_key order by timestamp_raw, record_id) as key_rn
    from post_after_t1
    where nik_key is not null
),

match_t2 as (
    select p.record_id as pre_record_id, q.record_id as post_record_id, 'T2_nik'::text as match_tier, 100::integer as match_score
    from pre_t2 p
    join post_t2 q on p.nik_key = q.nik_key and p.key_rn = q.key_rn
),

pre_after_t2 as (
    select p.* from pre_after_t1 p left join match_t2 m on p.record_id = m.pre_record_id where m.pre_record_id is null
),

post_after_t2 as (
    select q.* from post_after_t1 q left join match_t2 m on q.record_id = m.post_record_id where m.post_record_id is null
),

pre_t3 as (
    select *, row_number() over (partition by person_name_desa_pusk_key order by timestamp_raw, record_id) as key_rn
    from pre_after_t2
    where person_name_desa_pusk_key is not null
),

post_t3 as (
    select *, row_number() over (partition by person_name_desa_pusk_key order by timestamp_raw, record_id) as key_rn
    from post_after_t2
    where person_name_desa_pusk_key is not null
),

match_t3 as (
    select p.record_id as pre_record_id, q.record_id as post_record_id, 'T3_name_desa_pusk'::text as match_tier, 100::integer as match_score
    from pre_t3 p
    join post_t3 q on p.person_name_desa_pusk_key = q.person_name_desa_pusk_key and p.key_rn = q.key_rn
),

pre_after_t3 as (
    select p.* from pre_after_t2 p left join match_t3 m on p.record_id = m.pre_record_id where m.pre_record_id is null
),

post_after_t3 as (
    select q.* from post_after_t2 q left join match_t3 m on q.record_id = m.post_record_id where m.post_record_id is null
),

pre_t4 as (
    select *, row_number() over (partition by person_name_pusk_key order by timestamp_raw, record_id) as key_rn
    from pre_after_t3
    where person_name_pusk_key is not null
),

post_t4 as (
    select *, row_number() over (partition by person_name_pusk_key order by timestamp_raw, record_id) as key_rn
    from post_after_t3
    where person_name_pusk_key is not null
),

match_t4 as (
    select p.record_id as pre_record_id, q.record_id as post_record_id, 'T4_name_pusk'::text as match_tier, 100::integer as match_score
    from pre_t4 p
    join post_t4 q on p.person_name_pusk_key = q.person_name_pusk_key and p.key_rn = q.key_rn
),

pre_after_t4 as (
    select p.* from pre_after_t3 p left join match_t4 m on p.record_id = m.pre_record_id where m.pre_record_id is null
),

post_after_t4 as (
    select q.* from post_after_t3 q left join match_t4 m on q.record_id = m.post_record_id where m.post_record_id is null
),

pre_t5 as (
    select *, row_number() over (partition by person_name_key order by timestamp_raw, record_id) as key_rn
    from pre_after_t4
    where person_name_key is not null
),

post_t5 as (
    select *, row_number() over (partition by person_name_key order by timestamp_raw, record_id) as key_rn
    from post_after_t4
    where person_name_key is not null
),

match_t5 as (
    select p.record_id as pre_record_id, q.record_id as post_record_id, 'T5_name_only'::text as match_tier, 100::integer as match_score
    from pre_t5 p
    join post_t5 q on p.person_name_key = q.person_name_key and p.key_rn = q.key_rn
),

all_matches as (
    select * from match_t1
    union all select * from match_t2
    union all select * from match_t3
    union all select * from match_t4
    union all select * from match_t5
),

paired_rows as (
    select
        coalesce(p.nik_key, q.nik_key) as nik,
        cast(coalesce(p.timestamp_raw, q.timestamp_raw) as date) as date,
        initcap(coalesce(q.desa_raw, p.desa_raw)) as desa,
        initcap(
            case
                when length(coalesce(p.unified_name, '')) >= length(coalesce(q.unified_name, '')) and p.unified_name is not null then p.unified_name
                when q.unified_name is not null then q.unified_name
                when length(coalesce(p.nama_raw, '')) >= length(coalesce(q.nama_raw, '')) and p.nama_raw is not null then p.nama_raw
                else q.nama_raw
            end
        ) as nama,
        coalesce(q.usia, p.usia) as usia,
        coalesce(p.year, q.year) as year,
        case when p.score is not null and q.score is not null then q.score - p.score end as delta,
        case
            when p.nama_key is distinct from q.nama_key and p.person_name_key = q.person_name_key then 'matched after unified name standardization'
            when m.match_tier = 'T5_name_only' then 'matched on unified name only'
        end as notes,
        coalesce(q.peran_raw, p.peran_raw) as peran,
        coalesce(q.phone_key, p.phone_key) as phone,
        'paired'::text as status,
        case
            when p.score is not null and q.score is not null and q.score - p.score > 0 then 'Improved'
            when p.score is not null and q.score is not null and q.score - p.score < 0 then 'Declined'
            when p.score is not null and q.score is not null and q.score - p.score = 0 then 'Same'
        end as outcome,
        coalesce(p.quarter, q.quarter) as quarter,
        p.nama_raw as nama_pre,
        initcap(coalesce(q.posyandu_raw, p.posyandu_raw)) as posyandu,
        coalesce(q.provinsi_raw, p.provinsi_raw) as provinsi,
        coalesce(q.education, p.education) as education,
        initcap(coalesce(q.kabupaten_raw, p.kabupaten_raw)) as kabupaten,
        initcap(coalesce(q.kecamatan_raw, p.kecamatan_raw)) as kecamatan,
        q.nama_raw as nama_post,
        p.score as pre_score,
        initcap(coalesce(q.puskesmas_raw, p.puskesmas_raw)) as puskesmas,
        m.match_tier,
        q.score as post_score,
        m.match_score,
        coalesce(q.jenis_kelamin, p.jenis_kelamin) as jenis_kelamin,
        p.timestamp_raw as pre_timestamp,
        coalesce(q.peran_category, p.peran_category) as peran_category,
        q.timestamp_raw as post_timestamp
    from all_matches m
    join pre_dedup p on m.pre_record_id = p.record_id
    join post_dedup q on m.post_record_id = q.record_id
),

pre_only_rows as (
    select
        nik_key as nik,
        cast(timestamp_raw as date) as date,
        initcap(desa_raw) as desa,
        initcap(coalesce(unified_name, nama_raw)) as nama,
        usia,
        year,
        null::integer as delta,
        null::text as notes,
        peran_raw as peran,
        phone_key as phone,
        'pre_only'::text as status,
        null::text as outcome,
        quarter,
        nama_raw as nama_pre,
        initcap(posyandu_raw) as posyandu,
        provinsi_raw as provinsi,
        education,
        initcap(kabupaten_raw) as kabupaten,
        initcap(kecamatan_raw) as kecamatan,
        null::text as nama_post,
        score as pre_score,
        initcap(puskesmas_raw) as puskesmas,
        null::text as match_tier,
        null::integer as post_score,
        null::integer as match_score,
        jenis_kelamin,
        timestamp_raw as pre_timestamp,
        peran_category,
        null::timestamp as post_timestamp
    from pre_dedup p
    left join all_matches m on p.record_id = m.pre_record_id
    where m.pre_record_id is null
),

post_only_rows as (
    select
        nik_key as nik,
        cast(timestamp_raw as date) as date,
        initcap(desa_raw) as desa,
        initcap(coalesce(unified_name, nama_raw)) as nama,
        usia,
        year,
        null::integer as delta,
        null::text as notes,
        peran_raw as peran,
        phone_key as phone,
        'post_only'::text as status,
        null::text as outcome,
        quarter,
        null::text as nama_pre,
        initcap(posyandu_raw) as posyandu,
        provinsi_raw as provinsi,
        education,
        initcap(kabupaten_raw) as kabupaten,
        initcap(kecamatan_raw) as kecamatan,
        nama_raw as nama_post,
        null::integer as pre_score,
        initcap(puskesmas_raw) as puskesmas,
        null::text as match_tier,
        score as post_score,
        null::integer as match_score,
        jenis_kelamin,
        null::timestamp as pre_timestamp,
        peran_category,
        timestamp_raw as post_timestamp
    from post_dedup q
    left join all_matches m on q.record_id = m.post_record_id
    where m.post_record_id is null
)

select
    nik,
    date,
    desa,
    nama,
    usia,
    year,
    delta,
    notes,
    peran,
    phone,
    status,
    outcome,
    quarter,
    nama_pre,
    posyandu,
    provinsi,
    education,
    kabupaten,
    kecamatan,
    nama_post,
    pre_score,
    puskesmas,
    match_tier,
    post_score,
    match_score,
    jenis_kelamin,
    pre_timestamp,
    peran_category,
    post_timestamp
from paired_rows

union all

select * from pre_only_rows

union all

select * from post_only_rows
