-- Model: Cleans and standardizes cohort 14 Forms responses.
{% set question_columns = [
    '1__Apa_saja_manfaat_ASI_bagi_bayi_',
    '2__Bagaimana_prinsip_ASI_',
    '3__Dalam_menggunakan_lembar_ASI_mengenai_tanda_kecukupan_ASI__t',
    '4__Ketika_bayi_menyusu_pada_Ibu__pelekatan_yang_tepat_adalah_',
    '5___ASI_yang_terbaik_adalah_yang_keluarnya_terakhir__karena_bis',
    '6__Ketika_Ibu_sudah_bisa_memulai_masa_MPASI_untuk_anaknya__peny',
    '7__Saat_anak_sudah_memasuki_usia_9_bulan__maka_pemberian_makan_',
    '8__Dibawah_ini_adalah_contoh_pemberian_makan_yang_responsif_pad',
    '9__Ito_umur_10_bulan_menyukai_bubur_instan_dan_buah_buahan_kare',
    '10__Ibu_Kristin_saat_datang_ke_posyandu_mengatakan_sudah_memula',
    '11__Seorang_ibu_merasa_cemas_karena_anaknya_belum_bisa_melakuka',
    '12__Pada_bayi_usia_3_5_bulan__manakah_yang_termasuk_tanda_lapar',
    '13__Seorang_ibu_bercerita_bahwa_bayinya_yang_berusia_4_bulan_se',
    '14__Bayi_usia_7_bulan_menutup_mulut__memalingkan_kepala__dan_me',
    '15__Kondisi_manakah_yang_menjadi_tanda_pengasuh_perlu_segera_me',
    '16__Bahaya_ibu_hamil_yang_mengalami_tekanan_darah_tinggi_adalah',
    '17__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas__sistole',
    '18__Ibu_hamil_dikatakan_beresiko_darah_tinggi_jika',
    '19__Manfaat_Tablet_Tambah_Darah__TTD__adalah__',
    '20__Apa_yang_berisiko_terjadi_bila_ibu_hamil_mengalami_KEK_',
    '21__Berikut_merupakan_informasi_atau_penyuluhan_yang_dapat_dibe',
    '22__Di_grafik_berat_badan_anak_yang_ada_di_dalam_buku_KIA__terd',
    '23__Berat_badan_Steve_bulan_lalu_4400_gr__angka_KBM_bulan_ini_a',
    '24__Jika_kenaikan_berat_badan_anak_tidak_sesuai_KBM_maka_penyul',
    '25__Jika_kenaikan_berat_badan_tidak_sesuai_KBM__penyuluhan_utam',
    '26__Yang_perlu_dilakukan_kader_untuk_memantau_baduta_berat_bada'
] %}
{% set pre_relation = source('raw_sheets', 'training_14_forms_pre') %}
{% set post_relation = source('raw_sheets', 'training_14_forms_post') %}

{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=['staging', 'training_14', 'training']
) }}

-- Stack aligned pre-test and post-test form responses.
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
        cast({{ first_existing_column(relation, ['Pendidikan_Terakhir', 'Pendidikan Terakhir', 'Pendidikan terakhir']) }} as text) as education_raw,
        {% for col in question_columns %}
        cast("{{ col }}" as text) as q{{ loop.index }}_raw{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ relation }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

-- Clean raw fields and calculate matching keys.
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
        {{ validate_date('timestamp_raw_text') }} as timestamp_raw,
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

-- Standardize scores, phones, gender, education, and roles.
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
),

-- Pivot the reference answer key to one column per question.
answer_key as (
    select
        {% for col in question_columns %}
        max(nullif(btrim(lower(regexp_replace(correct_answer, '\s+', ' ', 'g'))), ''))
            filter (where question_no = {{ loop.index }}) as k{{ loop.index }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from reference.training_answer
    where form_code = '14_forms'
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
    kabupaten_key, kecamatan_key, puskesmas_key,
    {% for col in question_columns %}
    case when k{{ loop.index }} is not null then coalesce(q{{ loop.index }} = k{{ loop.index }}, false) end as q{{ loop.index }}_correct{% if not loop.last %},{% endif %}
    {% endfor %}
from normalized
cross join answer_key