{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=['staging', 'training_13', 'training']
) }}

-- Select only the agreed form columns before combining pre- and post-test
-- responses. form_tag preserves the source side for downstream pairing.
with recursive source_rows as (
    select
        row_number() over () as source_row_id,
        'pre'::text as form_tag,
        cast("Desa" as text) as desa_raw,
        cast("Nama" as text) as nama_raw,
        cast("Usia" as text) as usia_raw,
        cast("Peran" as text) as peran_raw,
        cast("Score" as text) as score_raw,
        cast("Provinsi" as text) as provinsi_raw,
        cast("Kabupaten" as text) as kabupaten_raw,
        cast("Kecamatan" as text) as kecamatan_raw,
        cast("Puskesmas" as text) as puskesmas_raw,
        cast("Timestamp" as text) as timestamp_raw_text,
        cast("Nomor_HP_WA" as text) as phone_raw,
        cast("Jenis_Kelamin" as text) as jenis_kelamin_raw,
        cast("Posyandu_Binaan" as text) as posyandu_raw,
        cast("Pendidikan_Terakhir" as text) as education_raw
    from {{ source('raw_sheets', 'training_13_forms_pre') }}

    union all

    select
        row_number() over () as source_row_id,
        'post'::text as form_tag,
        cast("Desa" as text) as desa_raw,
        cast("Nama" as text) as nama_raw,
        cast("Usia" as text) as usia_raw,
        cast("Peran" as text) as peran_raw,
        cast("Score" as text) as score_raw,
        cast("Provinsi" as text) as provinsi_raw,
        cast("Kabupaten" as text) as kabupaten_raw,
        cast("Kecamatan" as text) as kecamatan_raw,
        cast("Puskesmas" as text) as puskesmas_raw,
        cast("Timestamp" as text) as timestamp_raw_text,
        cast("Nomor_HP_WA" as text) as phone_raw,
        cast("Jenis_Kelamin" as text) as jenis_kelamin_raw,
        cast("Posyandu_Binaan" as text) as posyandu_raw,
        cast("Pendidikan_Terakhir" as text) as education_raw
    from {{ source('raw_sheets', 'training_13_forms_post') }}
),

-- Normalize blanks and calculate reusable matching keys once.
keyed as (
    select
        concat(form_tag, '_', source_row_id) as record_id,
        form_tag,
        source_row_id,
        nullif(trim(desa_raw), '') as desa_raw,
        nullif(trim(nama_raw), '') as nama_raw,
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

-- Apply the same field standardization used by training_12_forms_stg.
normalized as (
    select
        *,
        case
            when score_clean ~ '^[0-9]+(\.[0-9]+)?/[0-9]+(\.[0-9]+)?$'
                then cast(round(cast(split_part(score_clean, '/', 1) as numeric)
                     / nullif(cast(split_part(score_clean, '/', 2) as numeric), 0) * 100, 0) as integer)
            when score_clean ~ '^[0-9]+(\.[0-9]+)?$'
                then cast(round(cast(score_clean as numeric), 0) as integer)
        end as score,
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
    from keyed
),

name_observations as (
    select distinct nama_key, phone_key, desa_key, kabupaten_key, kecamatan_key, puskesmas_key
    from normalized
    where nama_key is not null
),

-- Name variants may link only when both identity context and spelling agree.
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
            order by length(coalesce(n.nama_raw, '')) desc, n.timestamp_raw asc nulls last, n.record_id
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
    provinsi_raw,
    kabupaten_raw,
    kecamatan_raw,
    puskesmas_raw,
    posyandu_raw,
    timestamp_raw,
    case when timestamp_raw is not null then extract(year from timestamp_raw)::integer end as year,
    case when timestamp_raw is not null then concat(extract(year from timestamp_raw)::integer, '-Q', extract(quarter from timestamp_raw)::integer) end as quarter,
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
