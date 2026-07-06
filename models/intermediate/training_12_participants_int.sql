-- training_12 pre/post matching (intermediate model)
-- Takes the staged pre- and post-test responses, deduplicates repeat
-- submissions within each form, then pairs each participant's pre response
-- with their post response using a tiered cascade of identifiers
-- (phone -> NIK -> name+desa+puskesmas -> name+puskesmas -> name only).
-- Output: one row per participant with pre/post scores, delta, outcome,
-- and status = paired / pre_only / post_only.

{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["intermediate", "training_12", "training"]
) }}

-- Matching tiers, tried in order of reliability. Each tier only sees records
-- not matched by an earlier tier. To add/remove/reorder a tier, edit this list.
{% set tiers = [
    {'tag': 'T1_phone',          'key': 'phone_key'},
    {'tag': 'T2_nik',            'key': 'nik_key'},
    {'tag': 'T3_name_desa_pusk', 'key': 'person_name_desa_pusk_key'},
    {'tag': 'T4_name_pusk',      'key': 'person_name_pusk_key'},
    {'tag': 'T5_name_only',      'key': 'person_name_key'},
] %}

-- base: pull staged rows and add the person-level matching keys. Split in two
-- so person_name_key is computed once instead of repeating the coalesce.
with base as (
    select *, coalesce(unified_name_key, nama_key) as person_name_key
    from {{ ref('training_12_forms_stg') }}
),

keyed as (
    select
        *,
        case when person_name_key is not null and desa_key is not null and puskesmas_key is not null
             then concat_ws('|', person_name_key, desa_key, puskesmas_key) end as person_name_desa_pusk_key,
        case when person_name_key is not null and puskesmas_key is not null
             then concat_ws('|', person_name_key, puskesmas_key) end as person_name_pusk_key
    from base
),

-- ranked/dedup: within each form, collapse duplicate submissions by the same
-- person. Identity = best available key (phone > nik > name+desa+pusk >
-- name+pusk > name > own row). `||` propagates null, so each candidate drops
-- out cleanly when its key is missing. Keep the highest score, newest first.
ranked as (
    select
        *,
        row_number() over (
            partition by form_tag, coalesce(
                'ph:'  || phone_key,
                'nik:' || nik_key,
                'ndp:' || person_name_desa_pusk_key,
                'np:'  || person_name_pusk_key,
                'n:'   || person_name_key,
                'row:' || record_id
            )
            order by score desc nulls last, timestamp_raw desc nulls last, record_id
        ) as dedupe_rn
    from keyed
),

-- pre_dedup / post_dedup: the surviving (best) row per person per form,
-- split into the two pools that the matching cascade will pair up.
pre_dedup as (select * from ranked where dedupe_rn = 1 and form_tag = 'pre'),
post_dedup as (select * from ranked where dedupe_rn = 1 and form_tag = 'post'),

-- Tiered matching cascade, generated per tier:
--   pre_tN / post_tN: rank remaining rows within each key value (key_rn makes
--     matching one-to-one when several people share a key — 1st pre pairs with
--     1st post, 2nd with 2nd, ...).
--   match_tN: pair pre and post on key + rank.
--   pre/post_after_tN: rows still unmatched, fed into the next tier.
{% for t in tiers %}
{% set i = loop.index %}
{% set pre_src = 'pre_dedup' if loop.first else 'pre_after_t' ~ (i - 1) %}
{% set post_src = 'post_dedup' if loop.first else 'post_after_t' ~ (i - 1) %}

pre_t{{ i }} as (
    select record_id, {{ t.key }},
        row_number() over (partition by {{ t.key }} order by timestamp_raw, record_id) as key_rn
    from {{ pre_src }}
    where {{ t.key }} is not null
),

post_t{{ i }} as (
    select record_id, {{ t.key }},
        row_number() over (partition by {{ t.key }} order by timestamp_raw, record_id) as key_rn
    from {{ post_src }}
    where {{ t.key }} is not null
),

match_t{{ i }} as (
    select
        p.record_id as pre_record_id,
        q.record_id as post_record_id,
        '{{ t.tag }}'::text as match_tier,
        100::integer as match_score
    from pre_t{{ i }} p
    join post_t{{ i }} q
        on p.{{ t.key }} = q.{{ t.key }} and p.key_rn = q.key_rn
),
{% if not loop.last %}

pre_after_t{{ i }} as (
    select p.* from {{ pre_src }} p
    where not exists (select 1 from match_t{{ i }} m where m.pre_record_id = p.record_id)
),

post_after_t{{ i }} as (
    select q.* from {{ post_src }} q
    where not exists (select 1 from match_t{{ i }} m where m.post_record_id = q.record_id)
),
{% endif %}
{% endfor %}

-- all_matches: every pre->post pairing found across all tiers. A record id can
-- appear at most once, because each tier only saw rows unmatched so far.
all_matches as (
    {% for t in tiers %}
    select * from match_t{{ loop.index }}{% if not loop.last %} union all{% endif %}
    {% endfor %}
),

-- spine: one row per output record — matched pairs, plus unmatched pre rows
-- (post side null) and unmatched post rows (pre side null). The final select
-- left-joins both sides, so every coalesce(q.x, p.x) naturally degrades to the
-- surviving side and paired/pre_only/post_only need no separate code paths.
spine as (
    select pre_record_id, post_record_id, match_tier, match_score from all_matches
    union all
    select p.record_id, null, null::text, null::integer
    from pre_dedup p
    where not exists (select 1 from all_matches m where m.pre_record_id = p.record_id)
    union all
    select null, q.record_id, null::text, null::integer
    from post_dedup q
    where not exists (select 1 from all_matches m where m.post_record_id = q.record_id)
)

-- Final select: one row per person outcome. Both joins are LEFT joins against
-- the spine, so:
--   paired    -> p and q both present: coalesced fields, delta = post - pre,
--                outcome Improved/Declined/Same, notes flag fuzzy-name matches
--   pre_only  -> q is null: post-side fields null, delta/outcome null
--   post_only -> p is null: pre-side fields null
select
    coalesce(p.nik_key, q.nik_key) as nik,
    cast(coalesce(p.timestamp_raw, q.timestamp_raw) as date) as date,
    initcap(coalesce(q.desa_raw, p.desa_raw)) as desa,
    -- display name: prefer the longer unified name, fall back to raw names
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
    case
        when p.record_id is not null and q.record_id is not null then 'paired'
        when p.record_id is not null then 'pre_only'
        else 'post_only'
    end as status,
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
from spine m
left join pre_dedup p on m.pre_record_id = p.record_id
left join post_dedup q on m.post_record_id = q.record_id