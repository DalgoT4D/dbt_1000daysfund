{% set pre_relation = source('raw_sheets', 'training_14_forms_pre') %}
{% set post_relation = source('raw_sheets', 'training_14_forms_post') %}

{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=['staging', 'training_14', 'training']
) }}

with source_rows as (
    {% for form_tag, relation in {'pre': pre_relation, 'post': post_relation}.items() %}
    select
        row_number() over () as source_row_id,
        '{{ form_tag }}'::text as form_tag,
        cast({{ first_existing_column(relation, ['Nama', 'Nama lengkap', 'Nama Lengkap']) }} as text) as nama_raw,
        cast({{ first_existing_column(relation, ['Desa']) }} as text) as desa_raw,
        cast({{ first_existing_column(relation, ['Usia', 'Umur']) }} as text) as usia_raw,
        cast({{ first_existing_column(relation, ['Peran', 'Peran anda']) }} as text) as peran_raw,
        cast({{ first_existing_column(relation, ['Score']) }} as text) as score_raw,
        cast({{ first_existing_column(relation, ['Provinsi', 'Mohon Pilih Provinsi', 'Pilih Provinsi']) }} as text) as provinsi_raw,
        cast({{ first_existing_column(relation, ['Kabupaten', 'Pilih Nama Kabupaten']) }} as text) as kabupaten_raw,
        cast({{ first_existing_column(relation, ['Kecamatan']) }} as text) as kecamatan_raw,
        cast({{ first_existing_column(relation, ['Puskesmas', 'Puskesmas Pengampu', 'Puskesmas pengampu']) }} as text) as puskesmas_raw,
        cast({{ first_existing_column(relation, ['Timestamp']) }} as text) as timestamp_raw_text,
        cast({{ first_existing_column(relation, ['Nomor_HP_WA', 'Nomor HP/WA', 'Nomor HP/Whatsapp']) }} as text) as phone_raw,
        cast({{ first_existing_column(relation, ['Jenis_Kelamin', 'Jenis Kelamin', 'Jenis kelamin']) }} as text) as jenis_kelamin_raw,
        cast({{ first_existing_column(relation, ['Posyandu_Binaan', 'Posyandu', 'Nama Posyandu', 'Asal Posyandu']) }} as text) as posyandu_raw,
        cast({{ first_existing_column(relation, ['Pendidikan_Terakhir', 'Pendidikan Terakhir', 'Pendidikan terakhir']) }} as text) as education_raw
    from {{ relation }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

keyed as (
    select
        concat(form_tag, '_', source_row_id) as record_id,
        form_tag, source_row_id,
        nullif(trim(nama_raw), '') as nama_raw,
        nullif(trim(desa_raw), '') as desa_raw,
        substring(nullif(trim(usia_raw), '') from '([0-9]+)') as usia,
        nullif(trim(peran_raw), '') as peran_raw,
        nullif(regexp_replace(trim(score_raw), '[[:space:]]+', '', 'g'), '') as score_clean,
        nullif(trim(provinsi_raw), '') as provinsi_raw,
        nullif(trim(kabupaten_raw), '') as kabupaten_raw,
        nullif(trim(kecamatan_raw), '') as kecamatan_raw,
        nullif(trim(puskesmas_raw), '') as puskesmas_raw,
        nullif(trim(phone_raw), '') as phone_raw,
        nullif(trim(jenis_kelamin_raw), '') as jenis_kelamin_raw,
        nullif(trim(posyandu_raw), '') as posyandu_raw,
        nullif(trim(education_raw), '') as education_raw,
        case
            when nullif(trim(timestamp_raw_text), '') is null then null
            when trim(timestamp_raw_text) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}:[0-9]{2}$'
                then cast(to_timestamp(trim(timestamp_raw_text), 'MM/DD/YYYY HH24:MI:SS') as timestamp)
            else cast(trim(timestamp_raw_text) as timestamp)
        end as timestamp_raw,
        {{ profile_name_key('nama_raw') }} as nama_key,
        {{ profile_name_key('desa_raw') }} as desa_key,
        {{ profile_name_key('kabupaten_raw') }} as kabupaten_key,
        {{ profile_name_key('kecamatan_raw') }} as kecamatan_key,
        {{ profile_name_key('puskesmas_raw') }} as puskesmas_key,
        {{ profile_name_key('jenis_kelamin_raw') }} as jenis_kelamin_key,
        {{ profile_name_key('education_raw') }} as education_key,
        {{ profile_name_key('peran_raw') }} as peran_key,
        {{ profile_whatsapp_key('phone_raw') }} as phone_digits
    from source_rows
),

normalized as (
    select
        *,
        case
            when score_clean ~ '^[0-9]+(\.[0-9]+)?/[0-9]+(\.[0-9]+)?$'
                then round(split_part(score_clean, '/', 1)::numeric / nullif(split_part(score_clean, '/', 2)::numeric, 0) * 100, 2)
            when score_clean ~ '^[0-9]+(\.[0-9]+)?$' then score_clean::numeric
        end as score,
        case
            when phone_digits is null or length(phone_digits) < 8 then null
            when phone_digits like '62%' then '0' || substr(phone_digits, 3)
            when phone_digits like '0%' then phone_digits
            else '0' || phone_digits
        end as phone_key,
        case when jenis_kelamin_key in ('laki laki', 'lakilaki') then 'laki_laki' when jenis_kelamin_key = 'perempuan' then 'perempuan' end as jenis_kelamin,
        case
            when education_key in ('tidak sekolah', 'tidak tamat sd', 'tidak tamat sd mi') then 'no_schooling'
            when education_key like 'sd%' then 'primary'
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
    from keyed
)

select
    record_id, form_tag, source_row_id, nama_raw,
    nama_raw as unified_name, nama_key as unified_name_key,
    desa_raw, usia, peran_raw, score, provinsi_raw, kabupaten_raw,
    kecamatan_raw, puskesmas_raw, posyandu_raw, timestamp_raw,
    case when timestamp_raw is not null then extract(year from timestamp_raw)::integer end as year,
    case when timestamp_raw is not null then concat(extract(year from timestamp_raw)::integer, '-Q', extract(quarter from timestamp_raw)::integer) end as quarter,
    phone_raw, phone_key, jenis_kelamin_raw, jenis_kelamin,
    education_raw, education, peran_category, nama_key, desa_key,
    kabupaten_key, kecamatan_key, puskesmas_key
from normalized
