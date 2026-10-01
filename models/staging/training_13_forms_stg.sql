-- Model: Cleans and standardizes cohort 13 Forms responses.
{% set question_columns = [
    '1__Apa_itu_STUNTING_',
    '2__Apa_bahaya_STUNTING_',
    '3__Mengapa_1000_hari_pertama_kehidupan_sering_disebut_masa_pent',
    '4__Ketika_memberikan_penyuluhan_dengan_Poster_Pintar__kader__na',
    '5__Poster_Pintar_merupakan_media_penyuluhan_STUNTING__Poster_pi',
    '6__Diketahui_umur_bayi_6_bulan_25_hari__Berapa_umur_penuh_saat_',
    '7__Jika_pada_grafik_panjang_badan_menurut_umur__hasil_pengukura',
    '8__Putra__laki_laki___saat_ini_berumur_1_tahun__panjang_badanny',
    '9__Nelis_umur_18_bulan__sudah_bisa_berdiri_tegak__pintar_jika_d',
    '10__Keluarga_bayi_Ani_baru_pindah_ke_desa_Kolbano__dan_baru_per',
    '11__Bagian_tubuh_mana_yang_wajib_menempel_saat_mengukur_tinggi_',
    '12__Manfaat_paling_utama_dalam_memantau_pertumbuhan_anak_secara',
    '13__Langkah_1_adalah___',
    '14__Langkah_2_adalah___',
    '15__Langkah_3_adalah___',
    '16__Langkah_4_adalah___',
    '17__Langkah_5_adalah___',
    '18__Apa_yang_dapat_dilakukan_setelah_hari_buka_posyandu_',
    '19__Ketika_sasaran_baduta_dilakukan_penimbangan_berat_badan_di_'
] %}
{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=['staging', 'training_13', 'training']
) }}

-- Select only the agreed form columns before combining pre- and post-test
-- responses. form_tag preserves the source side for downstream pairing.
-- Stack aligned pre-test and post-test form responses.
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
        cast("Pendidikan_Terakhir" as text) as education_raw,
        {% for col in question_columns %}
        cast("{{ col }}" as text) as q{{ loop.index }}_raw{% if not loop.last %},{% endif %}
        {% endfor %}
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
        cast("Pendidikan_Terakhir" as text) as education_raw,
        {% for col in question_columns %}
        cast("{{ col }}" as text) as q{{ loop.index }}_raw{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ source('raw_sheets', 'training_13_forms_post') }}
),

-- Normalize blanks and calculate reusable matching keys once.
-- Clean raw fields and calculate matching keys.
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
        {{ profile_whatsapp_key('phone_raw') }} as phone_digits,
        -- Normalize answers the same way reference.training_answer is normalized.
        {% for col in question_columns %}
        nullif(btrim(lower(regexp_replace(q{{ loop.index }}_raw, '\s+', ' ', 'g'))), '') as q{{ loop.index }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from source_rows
),

-- Apply the same field standardization used by training_12_forms_stg.
-- Standardize scores, phones, gender, education, and roles.
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

-- Collect distinct names with their identity context.
name_observations as (
    select distinct nama_key, phone_key, desa_key, kabupaten_key, kecamatan_key, puskesmas_key
    from normalized
    where nama_key is not null
),

-- Name variants may link only when both identity context and spelling agree.
-- Link likely spelling variants of the same person's name.
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

-- Traverse linked names into variant groups.
name_groups (nama_key, root) as (
    select nama_key, root from name_pairs
    union
    select np.nama_key, ng.root
    from name_pairs np
    join name_groups ng on np.root = ng.nama_key
),

-- Assign one stable group key to each name.
name_group_map as (
    select nama_key, min(root) as name_group_key
    from name_groups
    group by nama_key
),

-- Select one canonical display name per group.
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
),

-- Pivot the reference answer key to one column per question.
answer_key as (
    select
        {% for col in question_columns %}
        max(nullif(btrim(lower(regexp_replace(correct_answer, '\s+', ' ', 'g'))), ''))
            filter (where question_no = {{ loop.index }}) as k{{ loop.index }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from reference.training_answer
    where form_code = '13_forms'
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
    puskesmas_key,
    {% for col in question_columns %}
    case when k{{ loop.index }} is not null then coalesce(q{{ loop.index }} = k{{ loop.index }}, false) end as q{{ loop.index }}_correct{% if not loop.last %},{% endif %}
    {% endfor %}
from with_unified
cross join answer_key