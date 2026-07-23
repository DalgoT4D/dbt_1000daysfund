{% set answer_key = ['D', 'B', 'D', 'D', 'B', 'C', 'B', 'D', 'D', 'B', 'B', 'B', 'C', 'A', 'C', 'D', 'A', 'B', 'C', 'B', 'D'] %}

{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=['staging', 'training_14', 'training']
) }}

with source_rows as (
    {% for form_tag, relation in {
        'pre': source('raw_sheets', 'training_14_sheets_pre'),
        'post': source('raw_sheets', 'training_14_sheets_post')
    }.items() %}
    select
        row_number() over () as source_row_id,
        '{{ form_tag }}'::text as form_tag,
        cast("Nama" as text) as "Nama",
        cast("Desa" as text) as "Desa",
        cast("Usia" as text) as "Usia",
        cast("Peran" as text) as "Peran",
        cast("Provinsi" as text) as "Provinsi",
        cast("Kabupaten" as text) as "Kabupaten",
        cast("Kecamatan" as text) as "Kecamatan",
        cast("Puskesmas" as text) as "Puskesmas",
        cast("Nomor_HP_WA" as text) as "Nomor_HP_WA",
        cast("Jenis_Kelamin" as text) as "Jenis_Kelamin",
        cast("Posyandu_Binaan" as text) as "Posyandu_Binaan",
        cast("Pendidikan_Terakhir" as text) as "Pendidikan_Terakhir",
        cast({% if form_tag == 'post' %}"Tanggal_Pelatihan"{% else %}"Timestamp"{% endif %} as text) as timestamp_raw_text,
        {% for question_number in range(1, 22) %}
        cast(
            {{ first_existing_column(relation, ['Q' ~ question_number, 'q' ~ question_number]) }}
            as text
        ) as "Q{{ question_number }}"{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ relation }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

cleaned as (
    select
        concat(form_tag, '_', source_row_id) as record_id,
        form_tag,
        source_row_id,
        nullif(trim(cast("Nama" as text)), '') as nama_raw,
        nullif(trim(cast("Desa" as text)), '') as desa_raw,
        substring(nullif(trim(cast("Usia" as text)), '') from '([0-9]+)') as usia,
        nullif(trim(cast("Peran" as text)), '') as peran_raw,
        null::text as score_raw,
        nullif(trim(cast("Provinsi" as text)), '') as provinsi_raw,
        nullif(trim(cast("Kabupaten" as text)), '') as kabupaten_raw,
        nullif(trim(cast("Kecamatan" as text)), '') as kecamatan_raw,
        nullif(trim(cast("Puskesmas" as text)), '') as puskesmas_raw,
        nullif(trim(cast("Nomor_HP_WA" as text)), '') as phone_raw,
        nullif(trim(cast("Jenis_Kelamin" as text)), '') as jenis_kelamin_raw,
        nullif(trim(cast("Posyandu_Binaan" as text)), '') as posyandu_raw,
        nullif(trim(cast("Pendidikan_Terakhir" as text)), '') as education_raw,
        {{ validate_date('timestamp_raw_text') }} as training_date,
        {% for question_number in range(1, 22) %}
        upper(nullif(trim(cast("Q{{ question_number }}" as text)), '')) as q{{ question_number }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from source_rows
),

keyed as (
    select
        *,
        {{ profile_name_key('nama_raw') }} as nama_key,
        {{ profile_name_key('desa_raw') }} as desa_key,
        {{ profile_name_key('kabupaten_raw') }} as kabupaten_key,
        {{ profile_name_key('kecamatan_raw') }} as kecamatan_key,
        {{ profile_name_key('puskesmas_raw') }} as puskesmas_key,
        {{ profile_name_key('jenis_kelamin_raw') }} as jenis_kelamin_key,
        {{ profile_name_key('education_raw') }} as education_key,
        {{ profile_name_key('peran_raw') }} as peran_key,
        {{ profile_whatsapp_key('phone_raw') }} as phone_digits
    from cleaned
),

scored as (
    select
        *,
        ({% for answer in answer_key %}case when q{{ loop.index }} = '{{ answer }}' then 1 else 0 end{% if not loop.last %} + {% endif %}{% endfor %}) as correct_answer_count,
        ({% for answer in answer_key %}case when q{{ loop.index }} is not null then 1 else 0 end{% if not loop.last %} + {% endif %}{% endfor %}) as answered_question_count
    from keyed
),

normalized as (
    select
        *,
        round(correct_answer_count::numeric / {{ answer_key | length }} * 100, 2) as score,
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
            when education_key like 'sd%' or education_key in ('sekolah dasar sederajat', 'tamat sd mi', 'tamat sd sederajat') then 'primary'
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
)

select
    record_id, form_tag, source_row_id, nama_raw,
    nama_raw as unified_name,
    nama_key as unified_name_key,
    desa_raw, usia, peran_raw, score, correct_answer_count,
    answered_question_count, score_raw,
    {% for question_number in range(1, 22) %}q{{ question_number }},{% endfor %}
    provinsi_raw, kabupaten_raw, kecamatan_raw, puskesmas_raw, posyandu_raw,
    training_date,
    case when training_date is not null then extract(year from training_date)::integer end as year,
    case when training_date is not null then concat(extract(year from training_date)::integer, '-Q', extract(quarter from training_date)::integer) end as quarter,
    phone_raw, phone_key, jenis_kelamin_raw, jenis_kelamin,
    education_raw, education, peran_category, nama_key, desa_key,
    kabupaten_key, kecamatan_key, puskesmas_key
from normalized
