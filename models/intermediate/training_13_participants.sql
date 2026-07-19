{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=['intermediate', 'training_13', 'training']
) }}

with base as (
    select
        *,
        coalesce(
            case when phone_key is not null then 'phone:' || phone_key end,
            case when unified_name_key is not null and desa_key is not null and puskesmas_key is not null
                then 'name_desa_pusk:' || concat_ws('|', unified_name_key, desa_key, puskesmas_key) end,
            case when unified_name_key is not null and puskesmas_key is not null
                then 'name_pusk:' || concat_ws('|', unified_name_key, puskesmas_key) end,
            case when unified_name_key is not null then 'name:' || unified_name_key end,
            'row:' || record_id
        ) as participant_key
    from {{ ref('training_13_forms') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by form_tag, participant_key
            order by score desc nulls last, timestamp_raw desc nulls last, record_id
        ) as dedupe_rn
    from base
),

pre as (
    select * from ranked where form_tag = 'pre' and dedupe_rn = 1
),

post as (
    select * from ranked where form_tag = 'post' and dedupe_rn = 1
)

select
    coalesce(p.nik_key, q.nik_key) as nik,
    cast(coalesce(p.timestamp_raw, q.timestamp_raw) as date) as date,
    initcap(coalesce(q.desa_raw, p.desa_raw)) as desa,
    initcap(
        case
            when length(coalesce(p.unified_name, '')) >= length(coalesce(q.unified_name, ''))
                and p.unified_name is not null then p.unified_name
            when q.unified_name is not null then q.unified_name
            when length(coalesce(p.nama_raw, '')) >= length(coalesce(q.nama_raw, ''))
                and p.nama_raw is not null then p.nama_raw
            else q.nama_raw
        end
    ) as nama,
    coalesce(q.usia, p.usia) as usia,
    coalesce(p.year, q.year) as year,
    case when p.score is not null and q.score is not null then q.score - p.score end as delta,
    case
        when p.nama_key is distinct from q.nama_key
            and p.unified_name_key = q.unified_name_key
            then 'matched after unified name standardization'
        when coalesce(p.participant_key, q.participant_key) like 'name:%'
            then 'matched on unified name only'
    end as notes,
    coalesce(q.peran_raw, p.peran_raw) as peran,
    coalesce(q.phone_key, p.phone_key) as phone,
    case
        when p.record_id is not null and q.record_id is not null then 'paired'
        when p.record_id is not null then 'pre_only'
        else 'post_only'
    end as status,
    case
        when p.score is not null and q.score > p.score then 'Improved'
        when p.score is not null and q.score < p.score then 'Declined'
        when p.score is not null and q.score = p.score then 'Same'
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
    case
        when p.record_id is null or q.record_id is null then null
        when p.participant_key like 'phone:%' then 'T1_phone'
        when p.participant_key like 'name_desa_pusk:%' then 'T3_name_desa_pusk'
        when p.participant_key like 'name_pusk:%' then 'T4_name_pusk'
        when p.participant_key like 'name:%' then 'T5_name_only'
    end as match_tier,
    q.score as post_score,
    case when p.record_id is not null and q.record_id is not null then 100 end as match_score,
    coalesce(q.jenis_kelamin, p.jenis_kelamin) as jenis_kelamin,
    p.timestamp_raw as pre_timestamp,
    coalesce(q.peran_category, p.peran_category) as peran_category,
    q.timestamp_raw as post_timestamp
from pre p
full outer join post q on p.participant_key = q.participant_key
