{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true}
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
        "Provinsi"                                                        as provinsi_raw,
        "Kabupaten"                                                       as kabupaten_raw,
        "Kecamatan"                                                       as kecamatan_raw,
        "Desa"                                                            as desa_raw,
        "Puskesmas_Pengampu"                                              as puskesmas_pengampu_raw,
        "Nama_Posyandu_Binaan"                                            as nama_posyandu_binaan_raw,
        "Timestamp"                                                       as submission_timestamp_raw,
        -- Question 1: Which pregnant women are targeted for the case management support program?
        "1__Ibu_hamil_yang_menjadi_sasaran_dampingan_untuk_program_penan" as q1_raw,
        -- Question 2: Why should a pregnant woman's blood pressure be monitored?
        "2__Mengapa_ibu_hamil_perlu_dipantau_tekanan_darahnya_"           as q2_raw,
        -- Question 3: What can cause hypertension to increase during pregnancy?
        "3__Berikut_penyebab_peningkatan_hipertensi__darah_tinggi__selam" as q3_raw,
        -- Question 4: Which option is not recommended to help a mother avoid hypertension?
        "4__Mana_yang_bukan_saran_bagi_Ibu_agar_terhindar_dari_hipertens" as q4_raw,
        -- Question 5: Response captured for a pregnant participant with high systolic blood pressure.
        "5__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_sistole_1" as q5_raw,
        -- Question 6: Response captured for a pregnant participant with blood pressure above 140.
        "6__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_140__dan_" as q6_raw,
        -- Question 7: Response captured for a pregnant participant with blood pressure above 120.
        "7__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_120__dan_" as q7_raw,
        -- Question 8: When is a pregnant woman considered at risk of high blood pressure?
        "8__Ibu_hamil_dikatakan_beresiko_darah_tinggi_jika"               as q8_raw,
        -- Question 9: What risks can occur when a pregnant woman experiences chronic energy deficiency (KEK)?
        "9__Apa_yang_berisiko_terjadi_bila_ibu_hamil_mengalami_KEK_"      as q9_raw,
        -- Question 10: How do you monitor blood pressure?
        "10__Bagaimana_cara_memantau_tekanan_darah_"                      as q10_raw,
        -- Question 11: What should be done to keep a pregnant woman's blood pressure healthy?
        "11__Apa_yang_harus_dilakukan_agar_menjaga_tekanan_darah_ibu_"    as q11_raw,
        -- Question 12: Which option is not a benefit of iron supplement tablets (TTD)?
        "12__Yang_bukan_merupakan_manfaat_Tablet_Tambah_Darah__TTD__adal" as q12_raw,
        -- Question 13: Which option is not recommended advice for a pregnant woman with chronic energy deficiency (KEK)?
        "13__Yang_bukan_saran_yang_bisa_diberikan_bagi_ibu_dampingan_KEK" as q13_raw,
        -- Question 14: What are the benefits of calcium-rich foods or calcium supplements?
        "14__Apa_manfaat_konsumsi_makanan_atau_suplemen_minuman_kalsium_" as q14_raw,
        -- Question 15: Which information or counseling can be given to a pregnant woman with hypertension?
        "15__Berikut_merupakan_informasi_atau_penyuluhan_yang_dapat_dibe" as q15_raw,
        -- Question 16: What monitoring is needed for pregnant women targeted for hypertension support?
        "16__Monitoring_yang_perlu_dilakukan_untuk_ibu_hamil_sasaran_hip" as q16_raw,
        -- Question 17: Which children under two are targeted for the support program?
        "17__Yang_menjadi_sasaran_dampingan_untuk_bayi_di_bawah_dua_tahu" as q17_raw,
        -- Question 18: Why must slowed weight gain be monitored?
        "18__Perlambatan_berat_badan_perlu_dipantau_karena"               as q18_raw,
        -- Question 19: In the case management program, who should receive support for growth slowing?
        "19__Dalam_program_penanganan_kasus__perlambatan_pertumbuhan_dil" as q19_raw,
        -- Question 20: If a child's weight gain is not aligned with KBM, what counseling should be given?
        "20__Jika_kenaikan_berat_badan_anak_tidak_sesuai_KBM_maka_penyul" as q20_raw,
        -- Question 21: If weight gain is not aligned with KBM, what is the main counseling topic?
        "21__Jika_kenaikan_berat_badan_tidak_sesuai_KBM__penyuluhan_utam" as q21_raw,
        -- Question 22: Which growth status corresponds to weight that does not increase?
        "22__Berikut_adalah_status_pertumbuhan_berat_badan_Tidak_Naik__T" as q22_raw,
        -- Question 23: Which option is not a cause of growth slowing?
        "23__Berikut_yang_bukan_merupakan_penyebab_perlambatan_pertumbuh" as q23_raw,
        -- Question 24: What length gain is expected during the first year after birth?
        "24__Pertambahan_panjang_badan_anak_dalam_1_tahun_sejak_lahir__0" as q24_raw,
        -- Question 25: How can toddler growth be monitored?
        "25__Bagaimana_caranya_memantau_pertumbuhan_balita_"              as q25_raw,
        -- Question 26: What monitoring is needed for children under two with growth slowing?
        "26__Monitoring_yang_perlu_dilakukan_untuk_baduta_dengan_perlamb" as q26_raw,
        null::varchar                                                     as training_strengths_raw,
        null::varchar                                                     as training_satisfaction_raw,
        null::varchar                                                     as training_improvements_raw,
        null::varchar                                                     as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '09a_pre') }}

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
        "Provinsi"                                                        as provinsi_raw,
        "Kabupaten"                                                       as kabupaten_raw,
        "Kecamatan"                                                       as kecamatan_raw,
        "Desa"                                                            as desa_raw,
        "Puskesmas_Pengampu"                                              as puskesmas_pengampu_raw,
        "Nama_Posyandu_Binaan"                                            as nama_posyandu_binaan_raw,
        "Timestamp"                                                       as submission_timestamp_raw,
        "1__Ibu_hamil_yang_menjadi_sasaran_dampingan_untuk_program_penan" as q1_raw,
        "2__Mengapa_ibu_hamil_perlu_dipantau_tekanan_darahnya_"           as q2_raw,
        "3__Berikut_penyebab_peningkatan_hipertensi__darah_tinggi__selam" as q3_raw,
        "4__Mana_yang_bukan_saran_bagi_Ibu_agar_terhindar_dari_hipertens" as q4_raw,
        "5__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_sistole_1" as q5_raw,
        "6__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_140__dan_" as q6_raw,
        "7__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_120__dan_" as q7_raw,
        "8__Ibu_hamil_dikatakan_beresiko_darah_tinggi_jika"               as q8_raw,
        "9__Apa_yang_berisiko_terjadi_bila_ibu_hamil_mengalami_KEK_"      as q9_raw,
        "10__Bagaimana_cara_memantau_tekanan_darah_"                      as q10_raw,
        "11__Apa_yang_harus_dilakukan_agar_menjaga_tekanan_darah_ibu_"    as q11_raw,
        "12__Yang_bukan_merupakan_manfaat_Tablet_Tambah_Darah__TTD__adal" as q12_raw,
        "13__Yang_bukan_saran_yang_bisa_diberikan_bagi_ibu_dampingan_KEK" as q13_raw,
        "14__Apa_manfaat_konsumsi_makanan_atau_suplemen_minuman_kalsium_" as q14_raw,
        "15__Berikut_merupakan_informasi_atau_penyuluhan_yang_dapat_dibe" as q15_raw,
        "16__Monitoring_yang_perlu_dilakukan_untuk_ibu_hamil_sasaran_hip" as q16_raw,
        "17__Yang_menjadi_sasaran_dampingan_untuk_bayi_di_bawah_dua_tahu" as q17_raw,
        "18__Perlambatan_berat_badan_perlu_dipantau_karena"               as q18_raw,
        "19__Dalam_program_penanganan_kasus__perlambatan_pertumbuhan_dil" as q19_raw,
        "20__Jika_kenaikan_berat_badan_anak_tidak_sesuai_KBM_maka_penyul" as q20_raw,
        "21__Jika_kenaikan_berat_badan_tidak_sesuai_KBM__penyuluhan_utam" as q21_raw,
        "22__Berikut_adalah_status_pertumbuhan_berat_badan_Tidak_Naik__T" as q22_raw,
        "23__Berikut_yang_bukan_merupakan_penyebab_perlambatan_pertumbuh" as q23_raw,
        "24__Pertambahan_panjang_badan_anak_dalam_1_tahun_sejak_lahir__0" as q24_raw,
        "25__Bagaimana_caranya_memantau_pertumbuhan_balita_"              as q25_raw,
        "26__Monitoring_yang_perlu_dilakukan_untuk_baduta_dengan_perlamb" as q26_raw,
        -- Post-training feedback: What went well in today's training?
        "Apa_yang_sudah_baik_dari_pelatihan_hari_ini_"                    as training_strengths_raw,
        -- Post-training feedback: How satisfied are you with the training?
        "Sejauh_mana_kepuasan_yang_Bapak_Ibu_berikan_terhadap_pelatihan_"  as training_satisfaction_raw,
        -- Post-training feedback: What improvements should we make for future training?
        "Perbaikan_seperti_apa_yang_perlu_kami_lakukan_untuk_pelatihan_k" as training_improvements_raw,
        -- Post-training feedback: I feel confident applying the knowledge and/or skills from the training.
        "Saya_merasa_percaya_diri_untuk_menerapkan_pengetahuan_dan_atau_" as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '09a_post') }}

),

combined_source as (

    select * from pre_source
    union all
    select * from post_source

),

final as (

    select
        pre_post,

        -- Respondent details
        nullif(btrim(nik_raw), '')                                                        as nik,
        nullif(btrim(nama_raw), '')                                                       as nama,
        nullif(btrim(usia_raw), '')::numeric                                             as usia,
        nullif(btrim(peran_raw), '')                                                      as peran,
        case
            when nullif(btrim(split_part(coalesce(score_raw, ''), '/', 1)), '') ~ '^[0-9]+(\\.[0-9]+)?$'
                then nullif(btrim(split_part(coalesce(score_raw, ''), '/', 1)), '')::numeric
        end                                                                              as score,
        case
            when nullif(btrim(split_part(coalesce(score_raw, ''), '/', 2)), '') ~ '^[0-9]+(\\.[0-9]+)?$'
                then nullif(btrim(split_part(coalesce(score_raw, ''), '/', 2)), '')::numeric
        end                                                                              as max_score,
        nullif(btrim(jenis_kelamin_raw), '')                                              as jenis_kelamin,
        nullif(btrim(pendidikan_terakhir_raw), '')                                        as pendidikan_terakhir,
        nullif(btrim(nomor_hp_wa_raw), '')                                                as nomor_hp_wa,

        -- Location and assignment details
        nullif(btrim(provinsi_raw), '')                                                   as provinsi,
        nullif(btrim(kabupaten_raw), '')                                                  as kabupaten,
        nullif(btrim(kecamatan_raw), '')                                                  as kecamatan,
        nullif(btrim(desa_raw), '')                                                       as desa,
        nullif(btrim(puskesmas_pengampu_raw), '')                                         as puskesmas_pengampu,
        nullif(btrim(nama_posyandu_binaan_raw), '')                                       as nama_posyandu_binaan,
        nullif(btrim(submission_timestamp_raw), '')                                       as submission_timestamp,

        -- Knowledge questions
        nullif(btrim(q1_raw), '')                                                         as q1_sasaran_dampingan_ibu_hamil,
        nullif(btrim(q2_raw), '')                                                         as q2_alasan_pemantauan_tekanan_darah_ibu_hamil,
        nullif(btrim(q3_raw), '')                                                         as q3_penyebab_hipertensi_saat_hamil,
        nullif(btrim(q4_raw), '')                                                         as q4_bukan_saran_pencegahan_hipertensi,
        nullif(btrim(q5_raw), '')                                                         as q5_tindakan_tekanan_sistolik_tinggi,
        nullif(btrim(q6_raw), '')                                                         as q6_tindakan_tekanan_darah_atas_140,
        nullif(btrim(q7_raw), '')                                                         as q7_tindakan_tekanan_darah_atas_120,
        nullif(btrim(q8_raw), '')                                                         as q8_kriteria_risiko_darah_tinggi,
        nullif(btrim(q9_raw), '')                                                         as q9_risiko_ibu_hamil_kek,
        nullif(btrim(q10_raw), '')                                                        as q10_cara_memantau_tekanan_darah,
        nullif(btrim(q11_raw), '')                                                        as q11_menjaga_tekanan_darah_ibu,
        nullif(btrim(q12_raw), '')                                                        as q12_bukan_manfaat_ttd,
        nullif(btrim(q13_raw), '')                                                        as q13_bukan_saran_untuk_ibu_kek,
        nullif(btrim(q14_raw), '')                                                        as q14_manfaat_kalsium,
        nullif(btrim(q15_raw), '')                                                        as q15_penyuluhan_untuk_ibu_hipertensi,
        nullif(btrim(q16_raw), '')                                                        as q16_monitoring_ibu_hamil_hipertensi,
        nullif(btrim(q17_raw), '')                                                        as q17_sasaran_dampingan_baduta,
        nullif(btrim(q18_raw), '')                                                        as q18_alasan_memantau_perlambatan_berat_badan,
        nullif(btrim(q19_raw), '')                                                        as q19_sasaran_dampingan_perlambatan_pertumbuhan,
        nullif(btrim(q20_raw), '')                                                        as q20_penyuluhan_saat_kenaikan_bb_tidak_sesuai_kbm,
        nullif(btrim(q21_raw), '')                                                        as q21_penyuluhan_utama_saat_kenaikan_bb_tidak_sesuai_kbm,
        nullif(btrim(q22_raw), '')                                                        as q22_status_pertumbuhan_tidak_naik,
        nullif(btrim(q23_raw), '')                                                        as q23_bukan_penyebab_perlambatan_pertumbuhan,
        nullif(btrim(q24_raw), '')                                                        as q24_pertambahan_panjang_badan_tahun_pertama,
        nullif(btrim(q25_raw), '')                                                        as q25_cara_memantau_pertumbuhan_balita,
        nullif(btrim(q26_raw), '')                                                        as q26_monitoring_baduta_perlambatan_pertumbuhan,

        -- Post-training feedback, present only for post rows
        nullif(btrim(training_strengths_raw), '')                                         as training_strengths,
        nullif(btrim(training_satisfaction_raw), '')                                      as training_satisfaction,
        nullif(btrim(training_improvements_raw), '')                                      as training_improvements,
        nullif(btrim(confidence_to_apply_knowledge_or_skills_raw), '')                    as confidence_to_apply_knowledge_or_skills

    from combined_source

)

select *
from final
