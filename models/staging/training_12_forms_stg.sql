{% set pre_relation = source('raw_sheets', 'training_12_pre') %}
{% set post_relation = source('raw_sheets', 'training_12_post') %}

{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["staging", "training_12", "training"]
) }}

with recursive
{% for form_tag, relation in {'pre': pre_relation, 'post': post_relation}.items() %}
{{ form_tag }}_base as (
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
        cast({{ first_existing_column(relation, ['Nomor_HP_WA', 'Nomor HP/WA', 'Nomor HP/Whatsapp', 'Nomor Handhone/Whatsapp']) }} as text) as phone_raw,
        cast({{ first_existing_column(relation, ['Jenis_Kelamin', 'Jenis Kelamin', 'Jenis kelamin']) }} as text) as jenis_kelamin_raw,
        cast({{ first_existing_column(relation, ['NIK']) }} as text) as nik_raw,
        cast({{ first_existing_column(relation, ['Pendidikan_Terakhir', 'Pendidikan Terakhir', 'Pendidikan terakhir']) }} as text) as education_raw,
        cast({{ first_existing_column(relation, ['Posyandu', 'Nama Posyandu', 'Nama Posyandu Binaan', 'Asal Posyandu']) }} as text) as posyandu_raw
    from {{ relation }}
){% if not loop.last %},{% endif %}
{% endfor %},

all_rows as (
    select * from pre_base
    union all
    select * from post_base
),

normalized as (
    select
        concat(form_tag, '_', source_row_id) as record_id,
        form_tag,
        source_row_id,
        nullif(trim(nama_raw), '') as nama_raw,
        nullif(trim(desa_raw), '') as desa_raw,
        substring(nullif(trim(usia_raw), '') from '([0-9]+)') as usia,
        nullif(trim(peran_raw), '') as peran_raw,
        nullif(trim(provinsi_raw), '') as provinsi_raw,
        nullif(trim(kabupaten_raw), '') as kabupaten_raw,
        nullif(trim(kecamatan_raw), '') as kecamatan_raw,
        nullif(trim(puskesmas_raw), '') as puskesmas_raw,
        nullif(trim(phone_raw), '') as phone_raw,
        nullif(trim(jenis_kelamin_raw), '') as jenis_kelamin_raw,
        nullif(trim(nik_raw), '') as nik_raw,
        nullif(trim(education_raw), '') as education_raw,
        nullif(trim(posyandu_raw), '') as posyandu_raw,
        case
            when nullif(trim(timestamp_raw_text), '') is null then null
            when trim(timestamp_raw_text) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}:[0-9]{2}$' then cast(to_timestamp(trim(timestamp_raw_text), 'MM/DD/YYYY HH24:MI:SS') as timestamp)
            else cast(trim(timestamp_raw_text) as timestamp)
        end as timestamp_raw,
        case
            when nullif(regexp_replace(trim(score_raw), '[[:space:]]+', '', 'g'), '') ~ '^[0-9]+(\\.[0-9]+)?/[0-9]+(\\.[0-9]+)?$' then cast(round((cast(split_part(regexp_replace(trim(score_raw), '[[:space:]]+', '', 'g'), '/', 1) as numeric) / nullif(cast(split_part(regexp_replace(trim(score_raw), '[[:space:]]+', '', 'g'), '/', 2) as numeric), 0)) * 100, 0) as integer)
            when nullif(trim(score_raw), '') ~ '^[0-9]+(\\.[0-9]+)?$' then cast(round(cast(trim(score_raw) as numeric), 0) as integer)
            else null
        end as score,
        {{ profile_name_key('nama_raw') }} as nama_key,
        {{ profile_name_key('desa_raw') }} as desa_key,
        {{ profile_name_key('kabupaten_raw') }} as kabupaten_key,
        {{ profile_name_key('kecamatan_raw') }} as kecamatan_key,
        {{ profile_name_key('puskesmas_raw') }} as puskesmas_key,
        case
            when {{ profile_whatsapp_key('phone_raw') }} is null or length({{ profile_whatsapp_key('phone_raw') }}) < 8 then null
            when {{ profile_whatsapp_key('phone_raw') }} like '62%' then concat('0', substr({{ profile_whatsapp_key('phone_raw') }}, 3))
            when left({{ profile_whatsapp_key('phone_raw') }}, 1) = '0' then {{ profile_whatsapp_key('phone_raw') }}
            else concat('0', {{ profile_whatsapp_key('phone_raw') }})
        end as phone_key,
        case when {{ profile_whatsapp_key('nik_raw') }} is not null and length({{ profile_whatsapp_key('nik_raw') }}) >= 10 then {{ profile_whatsapp_key('nik_raw') }} end as nik_key,
        case
            when {{ profile_name_key('jenis_kelamin_raw') }} in ('laki laki', 'lakilaki') then 'laki_laki'
            when {{ profile_name_key('jenis_kelamin_raw') }} = 'perempuan' then 'perempuan'
        end as jenis_kelamin,
        case
            when {{ profile_name_key('education_raw') }} in ('tidak sekolah', 'tidak tamat sd', 'tidak tamat sd mi') then 'no_schooling'
            when {{ profile_name_key('education_raw') }} like 'sd%' or {{ profile_name_key('education_raw') }} in ('sekolah dasar sederajat', 'tamat sd mi', 'tamat sd sederajat') then 'primary'
            when {{ profile_name_key('education_raw') }} like 'smp%' or {{ profile_name_key('education_raw') }} like 'sltp%' then 'junior_secondary'
            when {{ profile_name_key('education_raw') }} like 'sma%' or {{ profile_name_key('education_raw') }} like 'slta%' then 'senior_secondary'
            when {{ profile_name_key('education_raw') }} ~ '(^| )(d1|d2|d3|d4|s1|s2|s3|diploma|sarjana|ners)( |$)' then 'higher_education'
        end as education,
        case
            when {{ profile_name_key('peran_raw') }} ~ '(bidan|perawat|gizi|tpg|tenaga kesehatan|nutrisionis|promkes|promosi kesehatan|pkb|plkb|sanitarian|pustu|kia)' then 'Health Worker'
            when {{ profile_name_key('peran_raw') }} ~ '(kader|kpm|dasawisma|daswisma|posyandu)' then 'Community Health Worker'
            when {{ profile_name_key('peran_raw') }} ~ '(^| )(tpk|pkk|sekdes|kepala desa|bpd|staf desa|perangkat desa|kasi)( |$)' then 'Task Force'
            else 'General'
        end as peran_category
    from all_rows
),

name_observations as (
    select distinct nama_key, nama_raw, phone_key, nik_key, desa_key, kabupaten_key, kecamatan_key, puskesmas_key
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
            or (a.nik_key is not null and a.nik_key = b.nik_key)
            or (a.puskesmas_key is not null and a.puskesmas_key = b.puskesmas_key)
            or (a.desa_key is not null and a.kabupaten_key is not null and a.desa_key = b.desa_key and a.kabupaten_key = b.kabupaten_key)
            or (a.kecamatan_key is not null and a.kabupaten_key is not null and a.kecamatan_key = b.kecamatan_key and a.kabupaten_key = b.kabupaten_key)
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

name_groups_final as (
    select nama_key, min(root) as name_group_key
    from (
        select nama_key, root from name_groups
        union all
        select ng.nama_key, ng2.root
        from name_groups ng
        join name_groups ng2 on ng.root = ng2.nama_key
    ) chained
    group by nama_key

    union all

    select no.nama_key, no.nama_key
    from (select distinct nama_key from name_observations) no
    where not exists (select 1 from name_groups ng where ng.nama_key = no.nama_key)
),

with_unified as (
    select
        n.*,
        first_value(n.nama_raw) over (
            partition by coalesce(ngf.name_group_key, n.nama_key, n.record_id)
            order by length(coalesce(n.nama_raw, '')) desc, n.timestamp_raw asc nulls last, n.record_id
            rows between unbounded preceding and unbounded following
        ) as unified_name
    from normalized n
    left join name_groups_final ngf on n.nama_key = ngf.nama_key
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
    nik_raw,
    nik_key,
    education_raw,
    education,
    peran_category,
    nama_key,
    desa_key,
    kabupaten_key,
    kecamatan_key,
    puskesmas_key
from with_unified
