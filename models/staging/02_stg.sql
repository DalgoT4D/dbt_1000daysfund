{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true}, tags=["02_stg", "staging"] 
) }}

with pre_source as (

    select
        'pre'::text                                                       as pre_post,
        "NIK"                                                             as nik_raw,
        "Nama"                                                            as nama_raw,
        "Usia"                                                            as usia_raw,
        "Peran"                                                           as peran_raw,
        "Score"                                                           as score_raw,
        "Jenis_Kelamin"                                                   as jenis_kelamin_raw,
        "Pendidikan_Terakhir"                                             as pendidikan_terakhir_raw,
        "Nomor_HP_WA"                                                     as nomor_hp_wa_raw,
        null::varchar                                                     as email_address_raw,
        "Pilih_Provinsi"                                                  as provinsi_raw,
        "Pilih_Nama_Kabupaten"                                            as kabupaten_raw,
        "Kecamatan"                                                       as kecamatan_raw,
        "Desa"                                                            as desa_raw,
        "Puskesmas_Pengampu"                                              as puskesmas_pengampu_raw,
        "Nama_Posyandu"                                                   as nama_posyandu_raw,
        "Timestamp"                                                       as submission_timestamp_raw,
        
        -- Knowledge questions
        "1__Berikut_ini_adalah_beberapa_hal_yang_terkait_dengan_pertumbu" as q1_raw,
        "2__Pertambahan_panjang_badan_anak_dalam_1_tahun_sejak_lahir__0_" as q2_raw,
        "3__Manfaat_paling_utama_dalam_memantau_pertumbuhan_anak__yaitu_" as q3_raw,
        "4__Alat_yang_digunakan_untuk_mengukur_panjang_badan_bayi_yang_b" as q4_raw,
        "5__Bagian_tubuh_mana_yang_wajib_menempel_pada_stadiometer_untuk" as q5_raw,
        "6__Jika_anak_berumur_di_bawah_2_tahun__cara_pengukuran_yang_dis" as q6_raw,
        "7__Jika_umur_bayi_6_bulan_29_hari__dalam_perhitungan_umur_bulan" as q7_raw,
        "8__Apakah_standar_panjang_tinggi_badan_anak_laki_laki_dan_perem" as q8_raw,
        "9__Ketika_Putra_berumur_13_bulan__apa_status_panjang_badannya_"  as q9_raw,
        "10__Ketika_Putra_berumur_15_bulan__apa_status_panjang_badannya_" as q10_raw,
        "11__Jika_panjang_badan_bayi_tepat_di__2_SD__standar_deviasi___a" as q11_raw,
        "12__Penyuluhan_apa_yang_paling_prioritas_disarankan_bagi_Ibu_de" as q12_raw,
        "13__Jika_Ibu_dengan_anak_0_5_bulan_datang_ke_Posyandu_membawa_s" as q13_raw,
        "14__Sesuai_dengan_status_panjang_badan_Putra_ketika_umur_15_bul" as q14_raw,
        "15__Tahapan_apa_yang_dapat_dilewati_selama_pengukuran_dan_penyu" as q15_raw,
        "16__Jika_terdapat_tren_pertumbuhan_yang_mengalami_penurunan_pad" as q16_raw,
        
        -- Nulls for post-training rows
        null::varchar                                                     as training_strengths_raw,
        null::varchar                                                     as training_satisfaction_raw,
        null::varchar                                                     as training_improvements_raw,
        null::varchar                                                     as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '02_pre') }}

),

post_source as (

    select
        'post'::text                                                      as pre_post,
        "NIK"                                                             as nik_raw,
        "Nama"                                                            as nama_raw,
        "Usia"                                                            as usia_raw,
        "Peran"                                                           as peran_raw,
        "Score"                                                           as score_raw,
        "Jenis_Kelamin"                                                   as jenis_kelamin_raw,
        "Pendidikan_Terakhir"                                             as pendidikan_terakhir_raw,
        "Nomor_HP_WA"                                                     as nomor_hp_wa_raw,
        "Email_Address"                                                   as email_address_raw,
        "Mohon_Pilih_Provinsi"                                            as provinsi_raw,
        "Pilih_Nama_Kabupaten"                                            as kabupaten_raw,
        "Kecamatan"                                                       as kecamatan_raw,
        "Desa"                                                            as desa_raw,
        "Puskesmas_Pengampu"                                              as puskesmas_pengampu_raw,
        "Nama_Posyandu"                                                   as nama_posyandu_raw,
        "Timestamp"                                                       as submission_timestamp_raw,
        
        -- Knowledge questions
        "1__Berikut_ini_adalah_beberapa_hal_yang_terkait_dengan_pertumbu" as q1_raw,
        "2__Pertambahan_panjang_badan_anak_dalam_1_tahun_sejak_lahir__0_" as q2_raw,
        "3__Manfaat_paling_utama_dalam_memantau_pertumbuhan_anak__yaitu_" as q3_raw,
        "4__Alat_yang_digunakan_untuk_mengukur_panjang_badan_bayi_yang_b" as q4_raw,
        "5__Bagian_tubuh_mana_yang_wajib_menempel_pada_stadiometer_untuk" as q5_raw,
        "6__Jika_anak_berumur_di_bawah_2_tahun__cara_pengukuran_yang_dis" as q6_raw,
        "7__Jika_umur_bayi_6_bulan_29_hari__dalam_perhitungan_umur_bulan" as q7_raw,
        "8__Apakah_standar_panjang_tinggi_badan_anak_laki_laki_dan_perem" as q8_raw,
        "9__Ketika_Putra_berumur_13_bulan__apa_status_panjang_badannya_"  as q9_raw,
        "10__Ketika_Putra_berumur_15_bulan__apa_status_panjang_badannya_" as q10_raw,
        "11__Jika_panjang_badan_bayi_tepat_di__2_SD__standar_deviasi___a" as q11_raw,
        "12__Penyuluhan_apa_yang_paling_prioritas_disarankan_bagi_Ibu_de" as q12_raw,
        "13__Jika_Ibu_dengan_anak_0_5_bulan_datang_ke_Posyandu_membawa_s" as q13_raw,
        "14__Sesuai_dengan_status_panjang_badan_Putra_ketika_umur_15_bul" as q14_raw,
        "15__Tahapan_apa_yang_dapat_dilewati_selama_pengukuran_dan_penyu" as q15_raw,
        "16__Jika_terdapat_tren_pertumbuhan_yang_mengalami_penurunan_pad" as q16_raw,
        
        -- Post-training feedback
        "Apa_yang_sudah_baik_dari_pelatihan_hari_ini_"                    as training_strengths_raw,
        "Sejauh_mana_kepuasan_yang_Bapak_Ibu_berikan_terhadap_pelatihan_" as training_satisfaction_raw,
        "Perbaikan_seperti_apa_yang_perlu_kami_lakukan_untuk_pelatihan_k" as training_improvements_raw,
        "Saya_merasa_percaya_diri_untuk_menerapkan_pengetahuan_dan_atau_" as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '02_post') }}

),

combined_source as (

    select * from pre_source
    union all
    select * from post_source

)

select
    pre_post,

    -- Respondent details
    nullif(btrim(nik_raw), '')                                                       as nik,
    nullif(btrim(nama_raw), '')                                                      as nama,
    nullif(btrim(usia_raw), '')::numeric                                             as usia,
    nullif(btrim(peran_raw), '')                                                     as peran,
    case
        when nullif(btrim(split_part(coalesce(score_raw, ''), '/', 1)), '') ~ '^[0-9]+(\.[0-9]+)?$'
            then nullif(btrim(split_part(coalesce(score_raw, ''), '/', 1)), '')::numeric
    end                                                                              as score,
    case
        when nullif(btrim(split_part(coalesce(score_raw, ''), '/', 2)), '') ~ '^[0-9]+(\.[0-9]+)?$'
            then nullif(btrim(split_part(coalesce(score_raw, ''), '/', 2)), '')::numeric
    end                                                                              as max_score,
    nullif(btrim(jenis_kelamin_raw), '')                                             as jenis_kelamin,
    nullif(btrim(pendidikan_terakhir_raw), '')                                       as pendidikan_terakhir,
    nullif(btrim(nomor_hp_wa_raw), '')                                               as nomor_hp_wa,
    nullif(btrim(email_address_raw), '')                                             as email_address,

    -- Location and assignment details
    nullif(btrim(provinsi_raw), '')                                                  as provinsi,
    nullif(btrim(kabupaten_raw), '')                                                 as kabupaten,
    nullif(btrim(kecamatan_raw), '')                                                 as kecamatan,
    nullif(btrim(desa_raw), '')                                                      as desa,
    nullif(btrim(puskesmas_pengampu_raw), '')                                        as puskesmas_pengampu,
    nullif(btrim(nama_posyandu_raw), '')                                             as nama_posyandu,
    nullif(btrim(submission_timestamp_raw), '')                                      as submission_timestamp,

    -- Knowledge questions
    nullif(btrim(q1_raw), '')                                                        as q1_hal_terkait_pertumbuhan,
    nullif(btrim(q2_raw), '')                                                        as q2_pertambahan_panjang_badan_1_tahun,
    nullif(btrim(q3_raw), '')                                                        as q3_manfaat_memantau_pertumbuhan,
    nullif(btrim(q4_raw), '')                                                        as q4_alat_ukur_panjang_badan,
    nullif(btrim(q5_raw), '')                                                        as q5_bagian_tubuh_menempel_stadiometer,
    nullif(btrim(q6_raw), '')                                                        as q6_cara_pengukuran_anak_di_bawah_2_tahun,
    nullif(btrim(q7_raw), '')                                                        as q7_perhitungan_umur_bulan,
    nullif(btrim(q8_raw), '')                                                        as q8_standar_panjang_tinggi_anak,
    nullif(btrim(q9_raw), '')                                                        as q9_status_panjang_putra_13_bulan,
    nullif(btrim(q10_raw), '')                                                       as q10_status_panjang_putra_15_bulan,
    nullif(btrim(q11_raw), '')                                                       as q11_panjang_badan_tepat_di_min_2_sd,
    nullif(btrim(q12_raw), '')                                                       as q12_penyuluhan_prioritas,
    nullif(btrim(q13_raw), '')                                                       as q13_penanganan_anak_0_5_bulan,
    nullif(btrim(q14_raw), '')                                                       as q14_tindakan_status_panjang_putra_15_bulan,
    nullif(btrim(q15_raw), '')                                                       as q15_tahapan_dilewati_pengukuran,
    nullif(btrim(q16_raw), '')                                                       as q16_tren_pertumbuhan_penurunan,

    -- Post-training feedback
    nullif(btrim(training_strengths_raw), '')                                        as training_strengths,
    nullif(btrim(training_satisfaction_raw), '')                                     as training_satisfaction,
    nullif(btrim(training_improvements_raw), '')                                     as training_improvements,
    nullif(btrim(confidence_to_apply_knowledge_or_skills_raw), '')                   as confidence_to_apply_knowledge_or_skills

from combined_source