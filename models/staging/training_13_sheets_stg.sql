{% set answer_key = ['B', 'C', 'D', 'D', 'C', 'B', 'C', 'C', 'B', 'B', 'C', 'C', 'D', 'C', 'B', 'E', 'A', 'D', 'C'] %}

{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=['staging', 'training_13', 'training']
) }}

-- The sheets have the same layout, so stack them first and clean the shared
-- columns once. form_tag preserves which test each response came from.
with recursive source_rows as (
    select
        row_number() over () as source_row_id,
        'pre'::text as form_tag,
        *
    from {{ source('raw_sheets', 'training_13_sheets_pre') }}

    union all

    select
        row_number() over () as source_row_id,
        'post'::text as form_tag,
        *
    from {{ source('raw_sheets', 'training_13_sheets_post') }}
),

-- Clean display fields and compute matching keys once. Question responses are
-- normalized to uppercase letters so whitespace/case do not affect scoring.
keyed as (
    select
        concat(form_tag, '_', source_row_id) as record_id,
        form_tag,
        source_row_id,
        nullif(trim(cast("Nama" as text)), '') as nama_raw,
        nullif(trim(cast("Desa" as text)), '') as desa_raw,
        substring(nullif(trim(cast("Usia" as text)), '') from '([0-9]+)')::integer as usia,
        nullif(trim(cast("Peran" as text)), '') as peran_raw,
        nullif(trim(cast("Skor" as text)), '') as score_raw,
        nullif(trim(cast("Provinsi" as text)), '') as provinsi_raw,
        nullif(trim(cast("Kabupaten" as text)), '') as kabupaten_raw,
        nullif(trim(cast("Kecamatan" as text)), '') as kecamatan_raw,
        nullif(trim(cast("Puskesmas" as text)), '') as puskesmas_raw,
        nullif(trim(cast("Nomor_HP_WA" as text)), '') as phone_raw,
        nullif(trim(cast("Jenis_Kelamin" as text)), '') as jenis_kelamin_raw,
        nullif(trim(cast("Posyandu_Binaan" as text)), '') as posyandu_raw,
        nullif(trim(cast("Pendidikan_Terakhir" as text)), '') as education_raw,
        {{ validate_date('"Tanggal_Pelatihan"') }} as training_date,
        {% for question_number in range(1, 20) %}
        upper(nullif(trim(cast("Q{{ question_number }}" as text)), '')) as q{{ question_number }}{% if not loop.last %},{% endif %}
        {% endfor %},
        {{ profile_name_key('"Nama"') }} as nama_key,
        {{ profile_name_key('"Desa"') }} as desa_key,
        {{ profile_name_key('"Kabupaten"') }} as kabupaten_key,
        {{ profile_name_key('"Kecamatan"') }} as kecamatan_key,
        {{ profile_name_key('"Puskesmas"') }} as puskesmas_key,
        {{ profile_name_key('"Jenis_Kelamin"') }} as jenis_kelamin_key,
        {{ profile_name_key('"Pendidikan_Terakhir"') }} as education_key,
        {{ profile_name_key('"Peran"') }} as peran_key,
        {{ profile_whatsapp_key('"Nomor_HP_WA"') }} as phone_digits
    from source_rows
),

-- Score from the answer key rather than trusting the submitted Skor column.
-- The denominator is always 19, so unanswered questions count as incorrect.
scored as (
    select
        *,
        (
            {% for correct_answer in answer_key %}
            case when q{{ loop.index }} = '{{ correct_answer }}' then 1 else 0 end{% if not loop.last %} +{% endif %}
            {% endfor %}
        ) as correct_answer_count,
        (
            {% for correct_answer in answer_key %}
            case when q{{ loop.index }} is not null then 1 else 0 end{% if not loop.last %} +{% endif %}
            {% endfor %}
        ) as answered_question_count
    from keyed
),

normalized as (
    select
        *,
        round(correct_answer_count::numeric / 19 * 100, 2) as score,
        case
            when phone_digits is null or length(phone_digits) < 8 then null
            when phone_digits like '62%' then '0' || substr(phone_digits, 3)
            when phone_digits like '0%' then phone_digits
            else '0' || phone_digits
        end as phone_key,
        case
            when jenis_kelamin_key in ('laki laki', 'lakilaki') then 'laki_laki'
            when jenis_kelamin_key = 'perempuan' then 'perempuan'
        end as jenis_kelamin,
        case
            when education_key in ('tidak sekolah', 'tidak tamat sd', 'tidak tamat sd mi') then 'no_schooling'
            when education_key like 'sd%'
                or education_key in ('sekolah dasar sederajat', 'tamat sd mi', 'tamat sd sederajat') then 'primary'
            when education_key like 'smp%' or education_key like 'sltp%' then 'junior_secondary'
            when education_key like 'sma%' or education_key like 'slta%' then 'senior_secondary'
            when education_key ~ '(^| )(d1|d2|d3|d4|s1|s2|s3|diploma|sarjana|ners)( |$)' then 'higher_education'
        end as education,
        case
            when peran_key ~ '(bidan|perawat|gizi|tpg|tenaga kesehatan|nutrisionis|promkes|promosi kesehatan|pkb|plkb|sanitarian|pustu|kia)' then 'Health Worker'
            when peran_key ~ '(kader|kpm|dasawisma|daswisma|posyandu)' then 'Community Health Worker'
            when peran_key ~ '(^| )(tpk|pkk|sekdes|kepala desa|bpd|staf desa|perangkat desa|kasi)( |$)' then 'Task Force'
            else 'General'
        end as peran_category
    from scored
),

-- Apply the cohort 12 name-unification approach: spelling variants can only
-- link when they share phone or geography and are also sufficiently similar.
name_observations as (
    select distinct nama_key, phone_key, desa_key, kabupaten_key, kecamatan_key, puskesmas_key
    from normalized
    where nama_key is not null
),

name_pairs as (
    select distinct a.nama_key, least(a.nama_key, b.nama_key) as root
    from name_observations a
    join name_observations b
        on a.nama_key <> b.nama_key
        and (
            (a.phone_key is not null and a.phone_key = b.phone_key)
            or (a.puskesmas_key is not null and a.puskesmas_key = b.puskesmas_key)
            or (a.desa_key is not null and a.kabupaten_key is not null
                and a.desa_key = b.desa_key and a.kabupaten_key = b.kabupaten_key)
            or (a.kecamatan_key is not null and a.kabupaten_key is not null
                and a.kecamatan_key = b.kecamatan_key and a.kabupaten_key = b.kabupaten_key)
        )
        and (
            similarity(a.nama_key, b.nama_key) >= 0.72
            or a.nama_key like b.nama_key || ' %'
            or b.nama_key like a.nama_key || ' %'
        )
),

name_groups (nama_key, root) as (
    select nama_key, root from name_pairs
    union
    select np.nama_key, ng.root
    from name_pairs np
    join name_groups ng on np.root = ng.nama_key
),

name_group_map as (
    select nama_key, min(root) as name_group_key
    from name_groups
    group by nama_key
),

with_unified as (
    select
        n.*,
        first_value(n.nama_raw) over (
            partition by coalesce(ngm.name_group_key, n.nama_key, n.record_id)
            order by length(coalesce(n.nama_raw, '')) desc, n.training_date asc nulls last, n.record_id
            rows between unbounded preceding and unbounded following
        ) as unified_name
    from normalized n
    left join name_group_map ngm on n.nama_key = ngm.nama_key
)

select
    record_id,
    form_tag,
    source_row_id,
    nama_raw,
    nullif(trim(unified_name), '') as unified_name,
    {{ profile_name_key('coalesce(unified_name, nama_raw)') }} as unified_name_key,
    desa_raw,
    usia,
    peran_raw,
    score,
    correct_answer_count,
    answered_question_count,
    score_raw,
    {% for question_number in range(1, 20) %}
    q{{ question_number }},{% endfor %}
    provinsi_raw,
    kabupaten_raw,
    kecamatan_raw,
    puskesmas_raw,
    posyandu_raw,
    training_date,
    case when training_date is not null then extract(year from training_date)::integer end as year,
    case when training_date is not null then concat(extract(year from training_date)::integer, '-Q', extract(quarter from training_date)::integer) end as quarter,
    phone_raw,
    phone_key,
    jenis_kelamin_raw,
    jenis_kelamin,
    education_raw,
    education,
    peran_category,
    nama_key,
    desa_key,
    kabupaten_key,
    kecamatan_key,
    puskesmas_key
from with_unified
