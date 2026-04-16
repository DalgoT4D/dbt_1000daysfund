{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true}
) }}

with pre_source as (

    select
        'pre'::text                                                        as pre_post,
        "NIK"                                                              as nik_raw,
        "Nama"                                                             as nama_raw,
        "Usia"                                                             as usia_raw,
        "Score"                                                            as score_raw,
        "Jenis_Kelamin"                                                    as jenis_kelamin_raw,
        "Pendidikan_Terakhir"                                              as pendidikan_terakhir_raw,
        "Nomor_HP_WA"                                                      as nomor_hp_wa_raw,
        "Provinsi"                                                         as provinsi_raw,
        "Kabupaten"                                                        as kabupaten_raw,
        "Kecamatan"                                                        as kecamatan_raw,
        "Desa"                                                             as desa_raw,
        "Puskesmas_Pengampu"                                               as puskesmas_pengampu_raw,
        "Nama_Posyandu_Binaan"                                             as nama_posyandu_binaan_raw,
        "Timestamp"                                                        as submission_timestamp_raw,

        -- Question 1: Which pregnant women are targeted for the case management support program?
        "1__Ibu_hamil_yang_menjadi_sasaran_dampingan_untuk_program_penan"  as q1_raw,
        -- Question 2: Why should a pregnant woman's blood pressure be monitored?
        "2__Mengapa_ibu_hamil_perlu_dipantau_tekanan_darahnya_"            as q2_raw,
        -- Question 3: What is the main advice given to pregnant women to avoid hypertension?
        "3__Saran_utama_yang_diberikan_pada_ibu_hamil_agar_terhindar_dar"  as q3_raw,
        -- Question 4: Response captured for a pregnant participant with high systolic blood pressure.
        "4__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_sistole_1"  as q4_raw,
        -- Question 5: Response captured for a pregnant participant with blood pressure above 140.
        "5__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_140__dan_"  as q5_raw,
        -- Question 6: Response captured for a pregnant participant with blood pressure above 120.
        "6__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_120__dan_"  as q6_raw,
        -- Question 7: When is a pregnant woman considered at risk of high blood pressure?
        "7__Ibu_hamil_dikatakan_beresiko_darah_tinggi_jika"                as q7_raw,
        -- Question 8: What risks can occur when a pregnant woman experiences chronic energy deficiency (KEK)?
        "8__Apa_yang_berisiko_terjadi_bila_ibu_hamil_mengalami_KEK_"       as q8_raw,
        -- Question 9: How do you monitor blood pressure?
        "9__Bagaimana_cara_memantau_tekanan_darah_"                        as q9_raw,
        -- Question 10: What should be done to keep a pregnant woman's blood pressure healthy?
        "10__Apa_yang_harus_dilakukan_agar_menjaga_tekanan_darah_ibu_"     as q10_raw,
        -- Question 11: How many times should a mother have pregnancy checkups at a health facility?
        "11__Berapa_kali_ibu_melakukan_pemeriksaan_kehamilan_di_fasilita"  as q11_raw,
        -- Question 12: What monitoring is needed for pregnant women targeted for hypertension support?
        "12__Monitoring_yang_perlu_dilakukan_untuk_ibu_hamil_sasaran_hip"  as q12_raw,
        -- Question 13: Which children under two are targeted for the support program?
        "13__Yang_menjadi_sasaran_dampingan_untuk_bayi_di_bawah_dua_tahu"  as q13_raw,
        -- Question 14: Why must slowed weight gain be monitored?
        "14__Perlambatan_berat_badan_perlu_dipantau_karena"                as q14_raw,
        -- Question 15: In the case management program, who should receive support for growth slowing?
        "15__Dalam_program_penanganan_kasus__perlambatan_pertumbuhan_dil"  as q15_raw,
        -- Question 16: If a child's weight gain is not aligned with KBM, what counseling should be given?
        "16__Jika_kenaikan_berat_badan_anak_tidak_sesuai_KBM_maka_penyul"  as q16_raw,
        -- Question 17: If weight gain is not aligned with KBM, what is the main counseling topic?
        "17__Jika_kenaikan_berat_badan_tidak_sesuai_KBM__penyuluhan_utam"  as q17_raw,
        -- Question 18: What length gain is expected during the first year after birth?
        "18__Pertambahan_panjang_badan_anak_dalam_1_tahun_sejak_lahir__0"  as q18_raw,
        -- Question 19: How can toddler growth be monitored?
        "19__Bagaimana_caranya_memantau_pertumbuhan_balita_"               as q19_raw,
        -- Question 20: What monitoring is needed for children under two with growth slowing?
        "20__Monitoring_yang_perlu_dilakukan_untuk_baduta_dengan_perlamb"  as q20_raw,

        null::varchar                                                      as training_strengths_raw,
        null::varchar                                                      as training_satisfaction_raw,
        null::varchar                                                      as training_improvements_raw,
        null::varchar                                                      as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '09b_pre') }}

),

post_source as (

    select
        'post'::text                                                       as pre_post,
        "NIK"                                                              as nik_raw,
        "Nama"                                                             as nama_raw,
        "Usia"                                                             as usia_raw,
        "Score"                                                            as score_raw,
        "Jenis_Kelamin"                                                    as jenis_kelamin_raw,
        "Pendidikan_Terakhir"                                              as pendidikan_terakhir_raw,
        "Nomor_HP_WA"                                                      as nomor_hp_wa_raw,
        "Provinsi"                                                         as provinsi_raw,
        "Kabupaten"                                                        as kabupaten_raw,
        "Kecamatan"                                                        as kecamatan_raw,
        "Desa"                                                             as desa_raw,
        "Puskesmas_Pengampu"                                               as puskesmas_pengampu_raw,
        "Nama_Posyandu_Binaan"                                             as nama_posyandu_binaan_raw,
        "Timestamp"                                                        as submission_timestamp_raw,

        "1__Ibu_hamil_yang_menjadi_sasaran_dampingan_untuk_program_penan"  as q1_raw,
        "2__Mengapa_ibu_hamil_perlu_dipantau_tekanan_darahnya_"            as q2_raw,
        "3__Saran_utama_yang_diberikan_pada_ibu_hamil_agar_terhindar_dar"  as q3_raw,
        "4__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_sistole_1"  as q4_raw,
        "5__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_140__dan_"  as q5_raw,
        "6__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas_120__dan_"  as q6_raw,
        "7__Ibu_hamil_dikatakan_beresiko_darah_tinggi_jika"                as q7_raw,
        "8__Apa_yang_berisiko_terjadi_bila_ibu_hamil_mengalami_KEK_"       as q8_raw,
        "9__Bagaimana_cara_memantau_tekanan_darah_"                        as q9_raw,
        "10__Apa_yang_harus_dilakukan_agar_menjaga_tekanan_darah_ibu_"     as q10_raw,
        "11__Berapa_kali_ibu_melakukan_pemeriksaan_kehamilan_di_fasilita"  as q11_raw,
        "12__Monitoring_yang_perlu_dilakukan_untuk_ibu_hamil_sasaran_hip"  as q12_raw,
        "13__Yang_menjadi_sasaran_dampingan_untuk_bayi_di_bawah_dua_tahu"  as q13_raw,
        "14__Perlambatan_berat_badan_perlu_dipantau_karena"                as q14_raw,
        "15__Dalam_program_penanganan_kasus__perlambatan_pertumbuhan_dil"  as q15_raw,
        "16__Jika_kenaikan_berat_badan_anak_tidak_sesuai_KBM_maka_penyul"  as q16_raw,
        "17__Jika_kenaikan_berat_badan_tidak_sesuai_KBM__penyuluhan_utam"  as q17_raw,
        "18__Pertambahan_panjang_badan_anak_dalam_1_tahun_sejak_lahir__0"  as q18_raw,
        "19__Bagaimana_caranya_memantau_pertumbuhan_balita_"               as q19_raw,
        "20__Monitoring_yang_perlu_dilakukan_untuk_baduta_dengan_perlamb"  as q20_raw,

        -- Post-training feedback: What went well in today's training?
        "Apa_yang_sudah_baik_dari_pelatihan_hari_ini_"                     as training_strengths_raw,
        -- Post-training feedback: How satisfied are you with the training?
        "Sejauh_mana_kepuasan_yang_Bapak_Ibu_berikan_terhadap_pelatihan_"  as training_satisfaction_raw,
        -- Post-training feedback: What improvements should we make for future training?
        "Perbaikan_seperti_apa_yang_perlu_kami_lakukan_untuk_pelatihan_k"  as training_improvements_raw,
        -- Post-training feedback: I feel confident applying the knowledge and/or skills from the training.
        "Saya_merasa_percaya_diri_untuk_menerapkan_pengetahuan_dan_atau_"  as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '09b_post') }}

),

combined_source as (

    select * from pre_source
    union all
    select * from post_source

),

final as (

    select
        pre_post,
        nullif(btrim(nik_raw), '')                                                         as nik,
        nullif(btrim(nama_raw), '')                                                        as nama,
        nullif(btrim(usia_raw), '')::numeric                                              as usia,
        case
            when nullif(btrim(split_part(coalesce(score_raw, ''), '/', 1)), '') ~ '^[0-9]+(\\.[0-9]+)?$'
                then nullif(btrim(split_part(coalesce(score_raw, ''), '/', 1)), '')::numeric
        end                                                                               as score,
        case
            when nullif(btrim(split_part(coalesce(score_raw, ''), '/', 2)), '') ~ '^[0-9]+(\\.[0-9]+)?$'
                then nullif(btrim(split_part(coalesce(score_raw, ''), '/', 2)), '')::numeric
        end                                                                               as max_score,
        nullif(btrim(jenis_kelamin_raw), '')                                               as jenis_kelamin,
        nullif(btrim(pendidikan_terakhir_raw), '')                                         as pendidikan_terakhir,
        nullif(btrim(nomor_hp_wa_raw), '')                                                 as nomor_hp_wa,

        nullif(btrim(provinsi_raw), '')                                                    as provinsi,
        nullif(btrim(kabupaten_raw), '')                                                   as kabupaten,
        nullif(btrim(kecamatan_raw), '')                                                   as kecamatan,
        nullif(btrim(desa_raw), '')                                                        as desa,
        nullif(btrim(puskesmas_pengampu_raw), '')                                          as puskesmas_pengampu,
        nullif(btrim(nama_posyandu_binaan_raw), '')                                        as nama_posyandu_binaan,
        nullif(btrim(submission_timestamp_raw), '')                                        as submission_timestamp,

        nullif(btrim(q1_raw), '')                                                          as q1_sasaran_dampingan_ibu_hamil,
        nullif(btrim(q2_raw), '')                                                          as q2_alasan_pemantauan_tekanan_darah_ibu_hamil,
        nullif(btrim(q3_raw), '')                                                          as q3_saran_utama_agar_terhindar_dari_hipertensi,
        nullif(btrim(q4_raw), '')                                                          as q4_tindakan_tekanan_sistolik_tinggi,
        nullif(btrim(q5_raw), '')                                                          as q5_tindakan_tekanan_darah_atas_140,
        nullif(btrim(q6_raw), '')                                                          as q6_tindakan_tekanan_darah_atas_120,
        nullif(btrim(q7_raw), '')                                                          as q7_kriteria_risiko_darah_tinggi,
        nullif(btrim(q8_raw), '')                                                          as q8_risiko_ibu_hamil_kek,
        nullif(btrim(q9_raw), '')                                                          as q9_cara_memantau_tekanan_darah,
        nullif(btrim(q10_raw), '')                                                         as q10_menjaga_tekanan_darah_ibu,
        nullif(btrim(q11_raw), '')                                                         as q11_frekuensi_pemeriksaan_kehamilan_di_fasilitas_kesehatan,
        nullif(btrim(q12_raw), '')                                                         as q12_monitoring_ibu_hamil_hipertensi,
        nullif(btrim(q13_raw), '')                                                         as q13_sasaran_dampingan_baduta,
        nullif(btrim(q14_raw), '')                                                         as q14_alasan_memantau_perlambatan_berat_badan,
        nullif(btrim(q15_raw), '')                                                         as q15_sasaran_dampingan_perlambatan_pertumbuhan,
        nullif(btrim(q16_raw), '')                                                         as q16_penyuluhan_saat_kenaikan_bb_tidak_sesuai_kbm,
        nullif(btrim(q17_raw), '')                                                         as q17_penyuluhan_utama_saat_kenaikan_bb_tidak_sesuai_kbm,
        nullif(btrim(q18_raw), '')                                                         as q18_pertambahan_panjang_badan_tahun_pertama,
        nullif(btrim(q19_raw), '')                                                         as q19_cara_memantau_pertumbuhan_balita,
        nullif(btrim(q20_raw), '')                                                         as q20_monitoring_baduta_perlambatan_pertumbuhan,

        nullif(btrim(training_strengths_raw), '')                                          as training_strengths,
        nullif(btrim(training_satisfaction_raw), '')                                       as training_satisfaction,
        nullif(btrim(training_improvements_raw), '')                                       as training_improvements,
        nullif(btrim(confidence_to_apply_knowledge_or_skills_raw), '')                     as confidence_to_apply_knowledge_or_skills

    from combined_source

)

select *
from final
