{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true}, tags=["12_stg", "staging"] 
) }}

with pre_source as (

    select
        'pre'::text                                                       as pre_post,
        "Nama"                                                            as nama_raw,
        "Usia"                                                            as usia_raw,
        "Peran"                                                           as peran_raw,
        "Score"                                                           as score_raw,
        "Jenis_Kelamin"                                                   as jenis_kelamin_raw,
        "Nomor_HP_WA"                                                     as nomor_hp_wa_raw,
        "Provinsi"                                                        as provinsi_raw,
        "Kabupaten"                                                       as kabupaten_raw,
        "Kecamatan"                                                       as kecamatan_raw,
        "Desa"                                                            as desa_raw,
        "Puskesmas"                                                       as puskesmas_raw,
        "Timestamp"                                                       as submission_timestamp_raw,
        
        -- Knowledge questions
        "1__Apa_bahaya_utama_stunting_"                                   as q1_raw,
        "2__Apa_yang_merupakan_AKIBAT_stunting_"                          as q2_raw,
        "3__Mana_pernyataan_yang_paling_tepat_terkait_ASI_eksklusif_"     as q3_raw,
        "4__Apa_yang_dapat_dilakukan_setelah_hari_buka_posyandu_"         as q4_raw,
        "5__Bagaimana_Langkah_pelayanan_hari_buka_posyandu_secara_beruru" as q5_raw,
        "6__Pada_langkah_penimbangan_dan_pengukuran_bagi_sasaran_bayi_ba" as q6_raw,
        "7__Makanan_tambahan_yang_disarankan_untuk_penyuluhan_di_Posyand" as q7_raw,
        "8__Ketika_melakukan_kunjungan_rumah__apa_saja_yang_harus_dilaku" as q8_raw,
        "9__Selama_berkunjung_ke_rumah__apa_langkah_yang_perlu_dilakukan" as q9_raw,
        "10__Manfaat_paling_utama_dalam_memantau_pertumbuhan_anak__yaitu" as q10_raw,
        "11__Bagian_tubuh_mana_yang_wajib_menempel_pada_stadiometer_untu" as q11_raw,
        "12__Alat_yang_digunakan_untuk_mengukur_panjang_badan_bayi_yang_" as q12_raw,
        "13__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas__sistole" as q13_raw,
        "14__Apa_yang_berisiko_terjadi_bila_ibu_hamil_mengalami_KEK_"     as q14_raw,
        "15__Berapa_kali_ibu_melakukan_pemeriksaan_kehamilan_di_fasilita" as q15_raw,
        "16__Monitoring_yang_perlu_dilakukan_untuk_ibu_hamil_sasaran_hip" as q16_raw,
        "17__Perlambatan_berat_badan_perlu_dipantau_karena"               as q17_raw,
        "18__Dalam_program_penanganan_kasus__perlambatan_pertumbuhan_dil" as q18_raw,
        "19__Jika_kenaikan_berat_badan_tidak_sesuai_KBM__penyuluhan_utam" as q19_raw,
        "20__Monitoring_yang_perlu_dilakukan_untuk_baduta_dengan_perlamb" as q20_raw,
        
        -- Pre-training expectations (Only in pre)
        "Apa_harapan_Anda_terhadap_kegiatan_pelatihan_Pokja_ini_"         as training_expectations_raw,
        "Apa_yang_akan_anda_khawatirkan_dari_mengikuti_pelatihan_Pokja_i" as training_worries_raw,

        -- Nulls for post-training rows
        null::varchar                                                     as training_strengths_raw,
        null::varchar                                                     as training_satisfaction_raw,
        null::varchar                                                     as training_improvements_raw,
        null::varchar                                                     as confidence_to_apply_knowledge_or_skills_raw,
        null::varchar                                                     as knowledge_relevance_raw

    from {{ source('raw_sheets', '12_pre') }}

),

post_source as (

    select
        'post'::text                                                      as pre_post,
        "Nama"                                                            as nama_raw,
        "Usia"                                                            as usia_raw,
        "Peran"                                                           as peran_raw,
        "Score"                                                           as score_raw,
        "Jenis_Kelamin"                                                   as jenis_kelamin_raw,
        "Nomor_HP_WA"                                                     as nomor_hp_wa_raw,
        "Provinsi"                                                        as provinsi_raw,
        "Kabupaten"                                                       as kabupaten_raw,
        "Kecamatan"                                                       as kecamatan_raw,
        "Desa"                                                            as desa_raw,
        "Puskesmas"                                                       as puskesmas_raw,
        "Timestamp"                                                       as submission_timestamp_raw,
        
        -- Knowledge questions
        "1__Apa_bahaya_utama_stunting_"                                   as q1_raw,
        "2__Apa_yang_merupakan_AKIBAT_stunting_"                          as q2_raw,
        "3__Mana_pernyataan_yang_paling_tepat_terkait_ASI_eksklusif_"     as q3_raw,
        "4__Apa_yang_dapat_dilakukan_setelah_hari_buka_posyandu_"         as q4_raw,
        "5__Bagaimana_Langkah_pelayanan_hari_buka_posyandu_secara_beruru" as q5_raw,
        "6__Pada_langkah_penimbangan_dan_pengukuran_bagi_sasaran_bayi_ba" as q6_raw,
        "7__Makanan_tambahan_yang_disarankan_untuk_penyuluhan_di_Posyand" as q7_raw,
        "8__Ketika_melakukan_kunjungan_rumah__apa_saja_yang_harus_dilaku" as q8_raw,
        "9__Selama_berkunjung_ke_rumah__apa_langkah_yang_perlu_dilakukan" as q9_raw,
        "10__Manfaat_paling_utama_dalam_memantau_pertumbuhan_anak__yaitu" as q10_raw,
        "11__Bagian_tubuh_mana_yang_wajib_menempel_pada_stadiometer_untu" as q11_raw,
        "12__Alat_yang_digunakan_untuk_mengukur_panjang_badan_bayi_yang_" as q12_raw,
        "13__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas__sistole" as q13_raw,
        "14__Apa_yang_berisiko_terjadi_bila_ibu_hamil_mengalami_KEK_"     as q14_raw,
        "15__Berapa_kali_ibu_melakukan_pemeriksaan_kehamilan_di_fasilita" as q15_raw,
        "16__Monitoring_yang_perlu_dilakukan_untuk_ibu_hamil_sasaran_hip" as q16_raw,
        "17__Perlambatan_berat_badan_perlu_dipantau_karena"               as q17_raw,
        "18__Dalam_program_penanganan_kasus__perlambatan_pertumbuhan_dil" as q18_raw,
        "19__Jika_kenaikan_berat_badan_tidak_sesuai_KBM__penyuluhan_utam" as q19_raw,
        "20__Monitoring_yang_perlu_dilakukan_untuk_baduta_dengan_perlamb" as q20_raw,
        
        -- Nulls for pre-training rows
        null::varchar                                                     as training_expectations_raw,
        null::varchar                                                     as training_worries_raw,

        -- Post-training feedback (Only in post)
        "Apa_yang_sudah_baik_dari_pelatihan_hari_ini_"                    as training_strengths_raw,
        "Sejauh_mana_kepuasan_yang_Bapak_Ibu_berikan_terhadap_pelatihan_" as training_satisfaction_raw,
        "Perbaikan_seperti_apa_yang_perlu_kami_lakukan_untuk_pelatihan_k" as training_improvements_raw,
        "Saya_merasa_percaya_diri_untuk_menerapkan_pengetahuan_dan_atau_" as confidence_to_apply_knowledge_or_skills_raw,
        "Saya_merasa_pengetahuan_dan_atau_keterampilan_baru_ini_relevan_" as knowledge_relevance_raw

    from {{ source('raw_sheets', '12_post') }}

),

combined_source as (

    select * from pre_source
    union all
    select * from post_source

)

select
    pre_post,

    -- Respondent details (Note: NIK, Nama_Posyandu, and Pendidikan_Terakhir were not in the 12 source schema)
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
    nullif(btrim(nomor_hp_wa_raw), '')                                               as nomor_hp_wa,

    -- Location and assignment details
    nullif(btrim(provinsi_raw), '')                                                  as provinsi,
    nullif(btrim(kabupaten_raw), '')                                                 as kabupaten,
    nullif(btrim(kecamatan_raw), '')                                                 as kecamatan,
    nullif(btrim(desa_raw), '')                                                      as desa,
    nullif(btrim(puskesmas_raw), '')                                                 as puskesmas,
    nullif(btrim(submission_timestamp_raw), '')                                      as submission_timestamp,

    -- Knowledge questions
    nullif(btrim(q1_raw), '')                                                        as q1_bahaya_utama_stunting,
    nullif(btrim(q2_raw), '')                                                        as q2_akibat_stunting,
    nullif(btrim(q3_raw), '')                                                        as q3_pernyataan_asi_eksklusif,
    nullif(btrim(q4_raw), '')                                                        as q4_tindakan_setelah_hari_buka_posyandu,
    nullif(btrim(q5_raw), '')                                                        as q5_langkah_pelayanan_posyandu,
    nullif(btrim(q6_raw), '')                                                        as q6_penimbangan_pengukuran_bayi,
    nullif(btrim(q7_raw), '')                                                        as q7_makanan_tambahan_penyuluhan,
    nullif(btrim(q8_raw), '')                                                        as q8_tindakan_kunjungan_rumah,
    nullif(btrim(q9_raw), '')                                                        as q9_langkah_kunjungan_rumah,
    nullif(btrim(q10_raw), '')                                                       as q10_manfaat_memantau_pertumbuhan,
    nullif(btrim(q11_raw), '')                                                       as q11_bagian_tubuh_menempel_stadiometer,
    nullif(btrim(q12_raw), '')                                                       as q12_alat_ukur_panjang_badan_bayi,
    nullif(btrim(q13_raw), '')                                                       as q13_tindakan_tekanan_sistolik_tinggi,
    nullif(btrim(q14_raw), '')                                                       as q14_risiko_ibu_hamil_kek,
    nullif(btrim(q15_raw), '')                                                       as q15_frekuensi_pemeriksaan_kehamilan,
    nullif(btrim(q16_raw), '')                                                       as q16_monitoring_ibu_hamil_hipertensi,
    nullif(btrim(q17_raw), '')                                                       as q17_alasan_memantau_perlambatan_berat_badan,
    nullif(btrim(q18_raw), '')                                                       as q18_penanganan_kasus_perlambatan_pertumbuhan,
    nullif(btrim(q19_raw), '')                                                       as q19_penyuluhan_utama_kenaikan_bb_tidak_sesuai_kbm,
    nullif(btrim(q20_raw), '')                                                       as q20_monitoring_baduta_perlambatan_pertumbuhan,

    -- Pre-training expectations
    nullif(btrim(training_expectations_raw), '')                                     as training_expectations,
    nullif(btrim(training_worries_raw), '')                                          as training_worries,

    -- Post-training feedback
    nullif(btrim(training_strengths_raw), '')                                        as training_strengths,
    nullif(btrim(training_satisfaction_raw), '')                                     as training_satisfaction,
    nullif(btrim(training_improvements_raw), '')                                     as training_improvements,
    nullif(btrim(confidence_to_apply_knowledge_or_skills_raw), '')                   as confidence_to_apply_knowledge_or_skills,
    nullif(btrim(knowledge_relevance_raw), '')                                       as knowledge_relevance

from combined_source