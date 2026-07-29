-- ============================================================================
-- Training quiz item analysis  (pre vs post, forms + sheets combined)
-- Answer key lives in reference.training_answer.
-- Training names are mapped inline in the training_meta CTE below — edit
-- there when a new cohort is added.
-- Post-survey feedback items (percaya_diri / kepuasan / sudah_baik) are
-- intentionally omitted so they don't collide with quiz questions 1-3.
-- ============================================================================

{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true},
    tags=["training_answers_stg", "staging"]
) }}

with training_meta (training, training_name) as (
    values
        (12, 'Kelompok Kerja'),
        (13, 'Poster Pintar, GC, Manajemen Posyandu'),
        (14, 'ASI, MPASI, ICCM')
),

answer_key as (
    select
        form_code, source_type, training, question_no, question_label,
        nullif(btrim(lower(regexp_replace(correct_answer, '\s+', ' ', 'g'))), '') as correct_answer_norm
    from reference.training_answer
),

responses as (
    -- training_12_pre  (20 questions)
    select '12' as form_code, 12 as training, 'pre' as stage,
           v.question_no, v.answer_norm
    from raw_sheets.training_12_pre,
    lateral (values
        (1, nullif(btrim(lower(regexp_replace("1__Apa_bahaya_utama_stunting_"::text, '\s+', ' ', 'g'))), '')),
        (2, nullif(btrim(lower(regexp_replace("2__Apa_yang_merupakan_AKIBAT_stunting_"::text, '\s+', ' ', 'g'))), '')),
        (3, nullif(btrim(lower(regexp_replace("3__Mana_pernyataan_yang_paling_tepat_terkait_ASI_eksklusif_"::text, '\s+', ' ', 'g'))), '')),
        (4, nullif(btrim(lower(regexp_replace("4__Apa_yang_dapat_dilakukan_setelah_hari_buka_posyandu_"::text, '\s+', ' ', 'g'))), '')),
        (5, nullif(btrim(lower(regexp_replace("5__Bagaimana_Langkah_pelayanan_hari_buka_posyandu_secara_beruru"::text, '\s+', ' ', 'g'))), '')),
        (6, nullif(btrim(lower(regexp_replace("6__Pada_langkah_penimbangan_dan_pengukuran_bagi_sasaran_bayi_ba"::text, '\s+', ' ', 'g'))), '')),
        (7, nullif(btrim(lower(regexp_replace("7__Makanan_tambahan_yang_disarankan_untuk_penyuluhan_di_Posyand"::text, '\s+', ' ', 'g'))), '')),
        (8, nullif(btrim(lower(regexp_replace("8__Ketika_melakukan_kunjungan_rumah__apa_saja_yang_harus_dilaku"::text, '\s+', ' ', 'g'))), '')),
        (9, nullif(btrim(lower(regexp_replace("9__Selama_berkunjung_ke_rumah__apa_langkah_yang_perlu_dilakukan"::text, '\s+', ' ', 'g'))), '')),
        (10, nullif(btrim(lower(regexp_replace("10__Manfaat_paling_utama_dalam_memantau_pertumbuhan_anak__yaitu"::text, '\s+', ' ', 'g'))), '')),
        (11, nullif(btrim(lower(regexp_replace("11__Bagian_tubuh_mana_yang_wajib_menempel_pada_stadiometer_untu"::text, '\s+', ' ', 'g'))), '')),
        (12, nullif(btrim(lower(regexp_replace("12__Alat_yang_digunakan_untuk_mengukur_panjang_badan_bayi_yang_"::text, '\s+', ' ', 'g'))), '')),
        (13, nullif(btrim(lower(regexp_replace("13__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas__sistole"::text, '\s+', ' ', 'g'))), '')),
        (14, nullif(btrim(lower(regexp_replace("14__Apa_yang_berisiko_terjadi_bila_ibu_hamil_mengalami_KEK_"::text, '\s+', ' ', 'g'))), '')),
        (15, nullif(btrim(lower(regexp_replace("15__Berapa_kali_ibu_melakukan_pemeriksaan_kehamilan_di_fasilita"::text, '\s+', ' ', 'g'))), '')),
        (16, nullif(btrim(lower(regexp_replace("16__Monitoring_yang_perlu_dilakukan_untuk_ibu_hamil_sasaran_hip"::text, '\s+', ' ', 'g'))), '')),
        (17, nullif(btrim(lower(regexp_replace("17__Perlambatan_berat_badan_perlu_dipantau_karena"::text, '\s+', ' ', 'g'))), '')),
        (18, nullif(btrim(lower(regexp_replace("18__Dalam_program_penanganan_kasus__perlambatan_pertumbuhan_dil"::text, '\s+', ' ', 'g'))), '')),
        (19, nullif(btrim(lower(regexp_replace("19__Jika_kenaikan_berat_badan_tidak_sesuai_KBM__penyuluhan_utam"::text, '\s+', ' ', 'g'))), '')),
        (20, nullif(btrim(lower(regexp_replace("20__Monitoring_yang_perlu_dilakukan_untuk_baduta_dengan_perlamb"::text, '\s+', ' ', 'g'))), ''))
    ) as v(question_no, answer_norm)
    union all
    -- training_12_post  (20 questions)
    select '12' as form_code, 12 as training, 'post' as stage,
           v.question_no, v.answer_norm
    from raw_sheets.training_12_post,
    lateral (values
        (1, nullif(btrim(lower(regexp_replace("1__Apa_bahaya_utama_stunting_"::text, '\s+', ' ', 'g'))), '')),
        (2, nullif(btrim(lower(regexp_replace("2__Apa_yang_merupakan_AKIBAT_stunting_"::text, '\s+', ' ', 'g'))), '')),
        (3, nullif(btrim(lower(regexp_replace("3__Mana_pernyataan_yang_paling_tepat_terkait_ASI_eksklusif_"::text, '\s+', ' ', 'g'))), '')),
        (4, nullif(btrim(lower(regexp_replace("4__Apa_yang_dapat_dilakukan_setelah_hari_buka_posyandu_"::text, '\s+', ' ', 'g'))), '')),
        (5, nullif(btrim(lower(regexp_replace("5__Bagaimana_Langkah_pelayanan_hari_buka_posyandu_secara_beruru"::text, '\s+', ' ', 'g'))), '')),
        (6, nullif(btrim(lower(regexp_replace("6__Pada_langkah_penimbangan_dan_pengukuran_bagi_sasaran_bayi_ba"::text, '\s+', ' ', 'g'))), '')),
        (7, nullif(btrim(lower(regexp_replace("7__Makanan_tambahan_yang_disarankan_untuk_penyuluhan_di_Posyand"::text, '\s+', ' ', 'g'))), '')),
        (8, nullif(btrim(lower(regexp_replace("8__Ketika_melakukan_kunjungan_rumah__apa_saja_yang_harus_dilaku"::text, '\s+', ' ', 'g'))), '')),
        (9, nullif(btrim(lower(regexp_replace("9__Selama_berkunjung_ke_rumah__apa_langkah_yang_perlu_dilakukan"::text, '\s+', ' ', 'g'))), '')),
        (10, nullif(btrim(lower(regexp_replace("10__Manfaat_paling_utama_dalam_memantau_pertumbuhan_anak__yaitu"::text, '\s+', ' ', 'g'))), '')),
        (11, nullif(btrim(lower(regexp_replace("11__Bagian_tubuh_mana_yang_wajib_menempel_pada_stadiometer_untu"::text, '\s+', ' ', 'g'))), '')),
        (12, nullif(btrim(lower(regexp_replace("12__Alat_yang_digunakan_untuk_mengukur_panjang_badan_bayi_yang_"::text, '\s+', ' ', 'g'))), '')),
        (13, nullif(btrim(lower(regexp_replace("13__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas__sistole"::text, '\s+', ' ', 'g'))), '')),
        (14, nullif(btrim(lower(regexp_replace("14__Apa_yang_berisiko_terjadi_bila_ibu_hamil_mengalami_KEK_"::text, '\s+', ' ', 'g'))), '')),
        (15, nullif(btrim(lower(regexp_replace("15__Berapa_kali_ibu_melakukan_pemeriksaan_kehamilan_di_fasilita"::text, '\s+', ' ', 'g'))), '')),
        (16, nullif(btrim(lower(regexp_replace("16__Monitoring_yang_perlu_dilakukan_untuk_ibu_hamil_sasaran_hip"::text, '\s+', ' ', 'g'))), '')),
        (17, nullif(btrim(lower(regexp_replace("17__Perlambatan_berat_badan_perlu_dipantau_karena"::text, '\s+', ' ', 'g'))), '')),
        (18, nullif(btrim(lower(regexp_replace("18__Dalam_program_penanganan_kasus__perlambatan_pertumbuhan_dil"::text, '\s+', ' ', 'g'))), '')),
        (19, nullif(btrim(lower(regexp_replace("19__Jika_kenaikan_berat_badan_tidak_sesuai_KBM__penyuluhan_utam"::text, '\s+', ' ', 'g'))), '')),
        (20, nullif(btrim(lower(regexp_replace("20__Monitoring_yang_perlu_dilakukan_untuk_baduta_dengan_perlamb"::text, '\s+', ' ', 'g'))), ''))
    ) as v(question_no, answer_norm)
    union all
    -- training_13_forms_pre  (19 questions)
    select '13_forms' as form_code, 13 as training, 'pre' as stage,
           v.question_no, v.answer_norm
    from raw_sheets.training_13_forms_pre,
    lateral (values
        (1, nullif(btrim(lower(regexp_replace("1__Apa_itu_STUNTING_"::text, '\s+', ' ', 'g'))), '')),
        (2, nullif(btrim(lower(regexp_replace("2__Apa_bahaya_STUNTING_"::text, '\s+', ' ', 'g'))), '')),
        (3, nullif(btrim(lower(regexp_replace("3__Mengapa_1000_hari_pertama_kehidupan_sering_disebut_masa_pent"::text, '\s+', ' ', 'g'))), '')),
        (4, nullif(btrim(lower(regexp_replace("4__Ketika_memberikan_penyuluhan_dengan_Poster_Pintar__kader__na"::text, '\s+', ' ', 'g'))), '')),
        (5, nullif(btrim(lower(regexp_replace("5__Poster_Pintar_merupakan_media_penyuluhan_STUNTING__Poster_pi"::text, '\s+', ' ', 'g'))), '')),
        (6, nullif(btrim(lower(regexp_replace("6__Diketahui_umur_bayi_6_bulan_25_hari__Berapa_umur_penuh_saat_"::text, '\s+', ' ', 'g'))), '')),
        (7, nullif(btrim(lower(regexp_replace("7__Jika_pada_grafik_panjang_badan_menurut_umur__hasil_pengukura"::text, '\s+', ' ', 'g'))), '')),
        (8, nullif(btrim(lower(regexp_replace("8__Putra__laki_laki___saat_ini_berumur_1_tahun__panjang_badanny"::text, '\s+', ' ', 'g'))), '')),
        (9, nullif(btrim(lower(regexp_replace("9__Nelis_umur_18_bulan__sudah_bisa_berdiri_tegak__pintar_jika_d"::text, '\s+', ' ', 'g'))), '')),
        (10, nullif(btrim(lower(regexp_replace("10__Keluarga_bayi_Ani_baru_pindah_ke_desa_Kolbano__dan_baru_per"::text, '\s+', ' ', 'g'))), '')),
        (11, nullif(btrim(lower(regexp_replace("11__Bagian_tubuh_mana_yang_wajib_menempel_saat_mengukur_tinggi_"::text, '\s+', ' ', 'g'))), '')),
        (12, nullif(btrim(lower(regexp_replace("12__Manfaat_paling_utama_dalam_memantau_pertumbuhan_anak_secara"::text, '\s+', ' ', 'g'))), '')),
        (13, nullif(btrim(lower(regexp_replace("13__Langkah_1_adalah___"::text, '\s+', ' ', 'g'))), '')),
        (14, nullif(btrim(lower(regexp_replace("14__Langkah_2_adalah___"::text, '\s+', ' ', 'g'))), '')),
        (15, nullif(btrim(lower(regexp_replace("15__Langkah_3_adalah___"::text, '\s+', ' ', 'g'))), '')),
        (16, nullif(btrim(lower(regexp_replace("16__Langkah_4_adalah___"::text, '\s+', ' ', 'g'))), '')),
        (17, nullif(btrim(lower(regexp_replace("17__Langkah_5_adalah___"::text, '\s+', ' ', 'g'))), '')),
        (18, nullif(btrim(lower(regexp_replace("18__Apa_yang_dapat_dilakukan_setelah_hari_buka_posyandu_"::text, '\s+', ' ', 'g'))), '')),
        (19, nullif(btrim(lower(regexp_replace("19__Ketika_sasaran_baduta_dilakukan_penimbangan_berat_badan_di_"::text, '\s+', ' ', 'g'))), ''))
    ) as v(question_no, answer_norm)
    union all
    -- training_13_forms_post  (19 questions)
    select '13_forms' as form_code, 13 as training, 'post' as stage,
           v.question_no, v.answer_norm
    from raw_sheets.training_13_forms_post,
    lateral (values
        (1, nullif(btrim(lower(regexp_replace("1__Apa_itu_STUNTING_"::text, '\s+', ' ', 'g'))), '')),
        (2, nullif(btrim(lower(regexp_replace("2__Apa_bahaya_STUNTING_"::text, '\s+', ' ', 'g'))), '')),
        (3, nullif(btrim(lower(regexp_replace("3__Mengapa_1000_hari_pertama_kehidupan_sering_disebut_masa_pent"::text, '\s+', ' ', 'g'))), '')),
        (4, nullif(btrim(lower(regexp_replace("4__Ketika_memberikan_penyuluhan_dengan_Poster_Pintar__kader__na"::text, '\s+', ' ', 'g'))), '')),
        (5, nullif(btrim(lower(regexp_replace("5__Poster_Pintar_merupakan_media_penyuluhan_STUNTING__Poster_pi"::text, '\s+', ' ', 'g'))), '')),
        (6, nullif(btrim(lower(regexp_replace("6__Diketahui_umur_bayi_6_bulan_25_hari__Berapa_umur_penuh_saat_"::text, '\s+', ' ', 'g'))), '')),
        (7, nullif(btrim(lower(regexp_replace("7__Jika_pada_grafik_panjang_badan_menurut_umur__hasil_pengukura"::text, '\s+', ' ', 'g'))), '')),
        (8, nullif(btrim(lower(regexp_replace("8__Putra__laki_laki___saat_ini_berumur_1_tahun__panjang_badanny"::text, '\s+', ' ', 'g'))), '')),
        (9, nullif(btrim(lower(regexp_replace("9__Nelis_umur_18_bulan__sudah_bisa_berdiri_tegak__pintar_jika_d"::text, '\s+', ' ', 'g'))), '')),
        (10, nullif(btrim(lower(regexp_replace("10__Keluarga_bayi_Ani_baru_pindah_ke_desa_Kolbano__dan_baru_per"::text, '\s+', ' ', 'g'))), '')),
        (11, nullif(btrim(lower(regexp_replace("11__Bagian_tubuh_mana_yang_wajib_menempel_saat_mengukur_tinggi_"::text, '\s+', ' ', 'g'))), '')),
        (12, nullif(btrim(lower(regexp_replace("12__Manfaat_paling_utama_dalam_memantau_pertumbuhan_anak_secara"::text, '\s+', ' ', 'g'))), '')),
        (13, nullif(btrim(lower(regexp_replace("13__Langkah_1_adalah___"::text, '\s+', ' ', 'g'))), '')),
        (14, nullif(btrim(lower(regexp_replace("14__Langkah_2_adalah___"::text, '\s+', ' ', 'g'))), '')),
        (15, nullif(btrim(lower(regexp_replace("15__Langkah_3_adalah___"::text, '\s+', ' ', 'g'))), '')),
        (16, nullif(btrim(lower(regexp_replace("16__Langkah_4_adalah___"::text, '\s+', ' ', 'g'))), '')),
        (17, nullif(btrim(lower(regexp_replace("17__Langkah_5_adalah___"::text, '\s+', ' ', 'g'))), '')),
        (18, nullif(btrim(lower(regexp_replace("18__Apa_yang_dapat_dilakukan_setelah_hari_buka_posyandu_"::text, '\s+', ' ', 'g'))), '')),
        (19, nullif(btrim(lower(regexp_replace("19__Ketika_sasaran_baduta_dilakukan_penimbangan_berat_badan_di_"::text, '\s+', ' ', 'g'))), ''))
    ) as v(question_no, answer_norm)
    union all
    -- training_13_sheets_pre  (19 questions)
    select '13_sheets' as form_code, 13 as training, 'pre' as stage,
           v.question_no, v.answer_norm
    from raw_sheets.training_13_sheets_pre,
    lateral (values
        (1, nullif(btrim(lower(regexp_replace("Q1"::text, '\s+', ' ', 'g'))), '')),
        (2, nullif(btrim(lower(regexp_replace("Q2"::text, '\s+', ' ', 'g'))), '')),
        (3, nullif(btrim(lower(regexp_replace("Q3"::text, '\s+', ' ', 'g'))), '')),
        (4, nullif(btrim(lower(regexp_replace("Q4"::text, '\s+', ' ', 'g'))), '')),
        (5, nullif(btrim(lower(regexp_replace("Q5"::text, '\s+', ' ', 'g'))), '')),
        (6, nullif(btrim(lower(regexp_replace("Q6"::text, '\s+', ' ', 'g'))), '')),
        (7, nullif(btrim(lower(regexp_replace("Q7"::text, '\s+', ' ', 'g'))), '')),
        (8, nullif(btrim(lower(regexp_replace("Q8"::text, '\s+', ' ', 'g'))), '')),
        (9, nullif(btrim(lower(regexp_replace("Q9"::text, '\s+', ' ', 'g'))), '')),
        (10, nullif(btrim(lower(regexp_replace("Q10"::text, '\s+', ' ', 'g'))), '')),
        (11, nullif(btrim(lower(regexp_replace("Q11"::text, '\s+', ' ', 'g'))), '')),
        (12, nullif(btrim(lower(regexp_replace("Q12"::text, '\s+', ' ', 'g'))), '')),
        (13, nullif(btrim(lower(regexp_replace("Q13"::text, '\s+', ' ', 'g'))), '')),
        (14, nullif(btrim(lower(regexp_replace("Q14"::text, '\s+', ' ', 'g'))), '')),
        (15, nullif(btrim(lower(regexp_replace("Q15"::text, '\s+', ' ', 'g'))), '')),
        (16, nullif(btrim(lower(regexp_replace("Q16"::text, '\s+', ' ', 'g'))), '')),
        (17, nullif(btrim(lower(regexp_replace("Q17"::text, '\s+', ' ', 'g'))), '')),
        (18, nullif(btrim(lower(regexp_replace("Q18"::text, '\s+', ' ', 'g'))), '')),
        (19, nullif(btrim(lower(regexp_replace("Q19"::text, '\s+', ' ', 'g'))), ''))
    ) as v(question_no, answer_norm)
    union all
    -- training_13_sheets_post  (19 questions)
    select '13_sheets' as form_code, 13 as training, 'post' as stage,
           v.question_no, v.answer_norm
    from raw_sheets.training_13_sheets_post,
    lateral (values
        (1, nullif(btrim(lower(regexp_replace("Q1"::text, '\s+', ' ', 'g'))), '')),
        (2, nullif(btrim(lower(regexp_replace("Q2"::text, '\s+', ' ', 'g'))), '')),
        (3, nullif(btrim(lower(regexp_replace("Q3"::text, '\s+', ' ', 'g'))), '')),
        (4, nullif(btrim(lower(regexp_replace("Q4"::text, '\s+', ' ', 'g'))), '')),
        (5, nullif(btrim(lower(regexp_replace("Q5"::text, '\s+', ' ', 'g'))), '')),
        (6, nullif(btrim(lower(regexp_replace("Q6"::text, '\s+', ' ', 'g'))), '')),
        (7, nullif(btrim(lower(regexp_replace("Q7"::text, '\s+', ' ', 'g'))), '')),
        (8, nullif(btrim(lower(regexp_replace("Q8"::text, '\s+', ' ', 'g'))), '')),
        (9, nullif(btrim(lower(regexp_replace("Q9"::text, '\s+', ' ', 'g'))), '')),
        (10, nullif(btrim(lower(regexp_replace("Q10"::text, '\s+', ' ', 'g'))), '')),
        (11, nullif(btrim(lower(regexp_replace("Q11"::text, '\s+', ' ', 'g'))), '')),
        (12, nullif(btrim(lower(regexp_replace("Q12"::text, '\s+', ' ', 'g'))), '')),
        (13, nullif(btrim(lower(regexp_replace("Q13"::text, '\s+', ' ', 'g'))), '')),
        (14, nullif(btrim(lower(regexp_replace("Q14"::text, '\s+', ' ', 'g'))), '')),
        (15, nullif(btrim(lower(regexp_replace("Q15"::text, '\s+', ' ', 'g'))), '')),
        (16, nullif(btrim(lower(regexp_replace("Q16"::text, '\s+', ' ', 'g'))), '')),
        (17, nullif(btrim(lower(regexp_replace("Q17"::text, '\s+', ' ', 'g'))), '')),
        (18, nullif(btrim(lower(regexp_replace("Q18"::text, '\s+', ' ', 'g'))), '')),
        (19, nullif(btrim(lower(regexp_replace("Q19"::text, '\s+', ' ', 'g'))), ''))
    ) as v(question_no, answer_norm)
    union all
    -- training_14_forms_pre  (21 questions)
    select '14_forms' as form_code, 14 as training, 'pre' as stage,
           v.question_no, v.answer_norm
    from raw_sheets.training_14_forms_pre,
    lateral (values
        (1, nullif(btrim(lower(regexp_replace("1__Apa_saja_manfaat_ASI_bagi_bayi_"::text, '\s+', ' ', 'g'))), '')),
        (2, nullif(btrim(lower(regexp_replace("2__Bagaimana_prinsip_ASI_"::text, '\s+', ' ', 'g'))), '')),
        (3, nullif(btrim(lower(regexp_replace("3__Dalam_menggunakan_lembar_ASI_mengenai_tanda_kecukupan_ASI__t"::text, '\s+', ' ', 'g'))), '')),
        (4, nullif(btrim(lower(regexp_replace("4__Ketika_bayi_menyusu_pada_Ibu__pelekatan_yang_tepat_adalah_"::text, '\s+', ' ', 'g'))), '')),
        (5, nullif(btrim(lower(regexp_replace("5___ASI_yang_terbaik_adalah_yang_keluarnya_terakhir__karena_bis"::text, '\s+', ' ', 'g'))), '')),
        (6, nullif(btrim(lower(regexp_replace("6__Ketika_Ibu_sudah_bisa_memulai_masa_MPASI_untuk_anaknya__peny"::text, '\s+', ' ', 'g'))), '')),
        (7, nullif(btrim(lower(regexp_replace("7__Saat_anak_sudah_memasuki_usia_9_bulan__maka_pemberian_makan_"::text, '\s+', ' ', 'g'))), '')),
        (8, nullif(btrim(lower(regexp_replace("8__Dibawah_ini_adalah_contoh_pemberian_makan_yang_responsif_pad"::text, '\s+', ' ', 'g'))), '')),
        (9, nullif(btrim(lower(regexp_replace("9__Ito_umur_10_bulan_menyukai_bubur_instan_dan_buah_buahan_kare"::text, '\s+', ' ', 'g'))), '')),
        (10, nullif(btrim(lower(regexp_replace("10__Ibu_Kristin_saat_datang_ke_posyandu_mengatakan_sudah_memula"::text, '\s+', ' ', 'g'))), '')),
        (11, nullif(btrim(lower(regexp_replace("11__Bahaya_ibu_hamil_yang_mengalami_tekanan_darah_tinggi_adalah"::text, '\s+', ' ', 'g'))), '')),
        (12, nullif(btrim(lower(regexp_replace("12__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas__sistole"::text, '\s+', ' ', 'g'))), '')),
        (13, nullif(btrim(lower(regexp_replace("13__Ibu_hamil_dikatakan_beresiko_darah_tinggi_jika"::text, '\s+', ' ', 'g'))), '')),
        (14, nullif(btrim(lower(regexp_replace("14__Manfaat_Tablet_Tambah_Darah__TTD__adalah__"::text, '\s+', ' ', 'g'))), '')),
        (15, nullif(btrim(lower(regexp_replace("15__Apa_yang_berisiko_terjadi_bila_ibu_hamil_mengalami_KEK_"::text, '\s+', ' ', 'g'))), '')),
        (16, nullif(btrim(lower(regexp_replace("16__Berikut_merupakan_informasi_atau_penyuluhan_yang_dapat_dibe"::text, '\s+', ' ', 'g'))), '')),
        (17, nullif(btrim(lower(regexp_replace("17__Di_grafik_berat_badan_anak_yang_ada_di_dalam_buku_KIA__terd"::text, '\s+', ' ', 'g'))), '')),
        (18, nullif(btrim(lower(regexp_replace("18__Berat_badan_Steve_bulan_lalu_4400_gr__angka_KBM_bulan_ini_a"::text, '\s+', ' ', 'g'))), '')),
        (19, nullif(btrim(lower(regexp_replace("19__Jika_kenaikan_berat_badan_anak_tidak_sesuai_KBM_maka_penyul"::text, '\s+', ' ', 'g'))), '')),
        (20, nullif(btrim(lower(regexp_replace("20__Jika_kenaikan_berat_badan_tidak_sesuai_KBM__penyuluhan_utam"::text, '\s+', ' ', 'g'))), '')),
        (21, nullif(btrim(lower(regexp_replace("21__Yang_perlu_dilakukan_kader_untuk_memantau_baduta_berat_bada"::text, '\s+', ' ', 'g'))), ''))
    ) as v(question_no, answer_norm)
    union all
    -- training_14_forms_post  (21 questions)
    select '14_forms' as form_code, 14 as training, 'post' as stage,
           v.question_no, v.answer_norm
    from raw_sheets.training_14_forms_post,
    lateral (values
        (1, nullif(btrim(lower(regexp_replace("1__Apa_saja_manfaat_ASI_bagi_bayi_"::text, '\s+', ' ', 'g'))), '')),
        (2, nullif(btrim(lower(regexp_replace("2__Bagaimana_prinsip_ASI_"::text, '\s+', ' ', 'g'))), '')),
        (3, nullif(btrim(lower(regexp_replace("3__Dalam_menggunakan_lembar_ASI_mengenai_tanda_kecukupan_ASI__t"::text, '\s+', ' ', 'g'))), '')),
        (4, nullif(btrim(lower(regexp_replace("4__Ketika_bayi_menyusu_pada_Ibu__pelekatan_yang_tepat_adalah_"::text, '\s+', ' ', 'g'))), '')),
        (5, nullif(btrim(lower(regexp_replace("5___ASI_yang_terbaik_adalah_yang_keluarnya_terakhir__karena_bis"::text, '\s+', ' ', 'g'))), '')),
        (6, nullif(btrim(lower(regexp_replace("6__Ketika_Ibu_sudah_bisa_memulai_masa_MPASI_untuk_anaknya__peny"::text, '\s+', ' ', 'g'))), '')),
        (7, nullif(btrim(lower(regexp_replace("7__Saat_anak_sudah_memasuki_usia_9_bulan__maka_pemberian_makan_"::text, '\s+', ' ', 'g'))), '')),
        (8, nullif(btrim(lower(regexp_replace("8__Dibawah_ini_adalah_contoh_pemberian_makan_yang_responsif_pad"::text, '\s+', ' ', 'g'))), '')),
        (9, nullif(btrim(lower(regexp_replace("9__Ito_umur_10_bulan_menyukai_bubur_instan_dan_buah_buahan_kare"::text, '\s+', ' ', 'g'))), '')),
        (10, nullif(btrim(lower(regexp_replace("10__Ibu_Kristin_saat_datang_ke_posyandu_mengatakan_sudah_memula"::text, '\s+', ' ', 'g'))), '')),
        (11, nullif(btrim(lower(regexp_replace("11__Bahaya_ibu_hamil_yang_mengalami_tekanan_darah_tinggi_adalah"::text, '\s+', ' ', 'g'))), '')),
        (12, nullif(btrim(lower(regexp_replace("12__Jika_sasaran_ibu_hamil_memiliki_tekanan_darah_atas__sistole"::text, '\s+', ' ', 'g'))), '')),
        (13, nullif(btrim(lower(regexp_replace("13__Ibu_hamil_dikatakan_beresiko_darah_tinggi_jika"::text, '\s+', ' ', 'g'))), '')),
        (14, nullif(btrim(lower(regexp_replace("14__Manfaat_Tablet_Tambah_Darah__TTD__adalah__"::text, '\s+', ' ', 'g'))), '')),
        (15, nullif(btrim(lower(regexp_replace("15__Apa_yang_berisiko_terjadi_bila_ibu_hamil_mengalami_KEK_"::text, '\s+', ' ', 'g'))), '')),
        (16, nullif(btrim(lower(regexp_replace("16__Berikut_merupakan_informasi_atau_penyuluhan_yang_dapat_dibe"::text, '\s+', ' ', 'g'))), '')),
        (17, nullif(btrim(lower(regexp_replace("17__Di_grafik_berat_badan_anak_yang_ada_di_dalam_buku_KIA__terd"::text, '\s+', ' ', 'g'))), '')),
        (18, nullif(btrim(lower(regexp_replace("18__Berat_badan_Steve_bulan_lalu_4400_gr__angka_KBM_bulan_ini_a"::text, '\s+', ' ', 'g'))), '')),
        (19, nullif(btrim(lower(regexp_replace("19__Jika_kenaikan_berat_badan_anak_tidak_sesuai_KBM_maka_penyul"::text, '\s+', ' ', 'g'))), '')),
        (20, nullif(btrim(lower(regexp_replace("20__Jika_kenaikan_berat_badan_tidak_sesuai_KBM__penyuluhan_utam"::text, '\s+', ' ', 'g'))), '')),
        (21, nullif(btrim(lower(regexp_replace("21__Yang_perlu_dilakukan_kader_untuk_memantau_baduta_berat_bada"::text, '\s+', ' ', 'g'))), ''))
    ) as v(question_no, answer_norm)
    union all
    -- training_14_sheets_pre  (21 questions)
    select '14_sheets' as form_code, 14 as training, 'pre' as stage,
           v.question_no, v.answer_norm
    from raw_sheets.training_14_sheets_pre,
    lateral (values
        (1, nullif(btrim(lower(regexp_replace("Q1"::text, '\s+', ' ', 'g'))), '')),
        (2, nullif(btrim(lower(regexp_replace("Q2"::text, '\s+', ' ', 'g'))), '')),
        (3, nullif(btrim(lower(regexp_replace("Q3"::text, '\s+', ' ', 'g'))), '')),
        (4, nullif(btrim(lower(regexp_replace("Q4"::text, '\s+', ' ', 'g'))), '')),
        (5, nullif(btrim(lower(regexp_replace("Q5"::text, '\s+', ' ', 'g'))), '')),
        (6, nullif(btrim(lower(regexp_replace("Q6"::text, '\s+', ' ', 'g'))), '')),
        (7, nullif(btrim(lower(regexp_replace("Q7"::text, '\s+', ' ', 'g'))), '')),
        (8, nullif(btrim(lower(regexp_replace("Q8"::text, '\s+', ' ', 'g'))), '')),
        (9, nullif(btrim(lower(regexp_replace("Q9"::text, '\s+', ' ', 'g'))), '')),
        (10, nullif(btrim(lower(regexp_replace("Q10"::text, '\s+', ' ', 'g'))), '')),
        (11, nullif(btrim(lower(regexp_replace("Q11"::text, '\s+', ' ', 'g'))), '')),
        (12, nullif(btrim(lower(regexp_replace("Q12"::text, '\s+', ' ', 'g'))), '')),
        (13, nullif(btrim(lower(regexp_replace("Q13"::text, '\s+', ' ', 'g'))), '')),
        (14, nullif(btrim(lower(regexp_replace("Q14"::text, '\s+', ' ', 'g'))), '')),
        (15, nullif(btrim(lower(regexp_replace("Q15"::text, '\s+', ' ', 'g'))), '')),
        (16, nullif(btrim(lower(regexp_replace("Q16"::text, '\s+', ' ', 'g'))), '')),
        (17, nullif(btrim(lower(regexp_replace("Q17"::text, '\s+', ' ', 'g'))), '')),
        (18, nullif(btrim(lower(regexp_replace("Q18"::text, '\s+', ' ', 'g'))), '')),
        (19, nullif(btrim(lower(regexp_replace("Q19"::text, '\s+', ' ', 'g'))), '')),
        (20, nullif(btrim(lower(regexp_replace("Q20"::text, '\s+', ' ', 'g'))), '')),
        (21, nullif(btrim(lower(regexp_replace("Q21"::text, '\s+', ' ', 'g'))), ''))
    ) as v(question_no, answer_norm)
    union all
    -- training_14_sheets_post  (21 questions)
    select '14_sheets' as form_code, 14 as training, 'post' as stage,
           v.question_no, v.answer_norm
    from raw_sheets.training_14_sheets_post,
    lateral (values
        (1, nullif(btrim(lower(regexp_replace("Q1"::text, '\s+', ' ', 'g'))), '')),
        (2, nullif(btrim(lower(regexp_replace("Q2"::text, '\s+', ' ', 'g'))), '')),
        (3, nullif(btrim(lower(regexp_replace("Q3"::text, '\s+', ' ', 'g'))), '')),
        (4, nullif(btrim(lower(regexp_replace("Q4"::text, '\s+', ' ', 'g'))), '')),
        (5, nullif(btrim(lower(regexp_replace("Q5"::text, '\s+', ' ', 'g'))), '')),
        (6, nullif(btrim(lower(regexp_replace("Q6"::text, '\s+', ' ', 'g'))), '')),
        (7, nullif(btrim(lower(regexp_replace("Q7"::text, '\s+', ' ', 'g'))), '')),
        (8, nullif(btrim(lower(regexp_replace("Q8"::text, '\s+', ' ', 'g'))), '')),
        (9, nullif(btrim(lower(regexp_replace("Q9"::text, '\s+', ' ', 'g'))), '')),
        (10, nullif(btrim(lower(regexp_replace("Q10"::text, '\s+', ' ', 'g'))), '')),
        (11, nullif(btrim(lower(regexp_replace("Q11"::text, '\s+', ' ', 'g'))), '')),
        (12, nullif(btrim(lower(regexp_replace("Q12"::text, '\s+', ' ', 'g'))), '')),
        (13, nullif(btrim(lower(regexp_replace("Q13"::text, '\s+', ' ', 'g'))), '')),
        (14, nullif(btrim(lower(regexp_replace("Q14"::text, '\s+', ' ', 'g'))), '')),
        (15, nullif(btrim(lower(regexp_replace("Q15"::text, '\s+', ' ', 'g'))), '')),
        (16, nullif(btrim(lower(regexp_replace("Q16"::text, '\s+', ' ', 'g'))), '')),
        (17, nullif(btrim(lower(regexp_replace("Q17"::text, '\s+', ' ', 'g'))), '')),
        (18, nullif(btrim(lower(regexp_replace("Q18"::text, '\s+', ' ', 'g'))), '')),
        (19, nullif(btrim(lower(regexp_replace("Q19"::text, '\s+', ' ', 'g'))), '')),
        (20, nullif(btrim(lower(regexp_replace("Q20"::text, '\s+', ' ', 'g'))), '')),
        (21, nullif(btrim(lower(regexp_replace("Q21"::text, '\s+', ' ', 'g'))), ''))
    ) as v(question_no, answer_norm)
),

scored as (
    select
        r.training, r.stage, r.question_no,
        (r.answer_norm is not null)                                          as is_answered,
        (r.answer_norm is not null and r.answer_norm = k.correct_answer_norm) as is_correct
    from responses r
    left join answer_key k
        on k.form_code = r.form_code and k.question_no = r.question_no
),

labels as (
    select training, question_no,
        coalesce(max(question_label) filter (where source_type = 'forms'),
                 max(question_label)) as question_label
    from answer_key
    group by 1, 2
),

agg as (
    select training, question_no,
        count(*) filter (where stage = 'pre'  and is_answered) as n_pre,
        count(*) filter (where stage = 'pre'  and is_correct)  as n_true_pre,
        count(*) filter (where stage = 'post' and is_answered) as n_post,
        count(*) filter (where stage = 'post' and is_correct)  as n_true_post
    from scored
    group by 1, 2
)

select
    l.training,
    m.training_name,
    l.question_no,
    l.question_label,
    coalesce(a.n_pre, 0)                                    as n_pre,
    coalesce(a.n_true_pre, 0)                               as n_true_pre,
    round(100.0 * a.n_true_pre  / nullif(a.n_pre,  0), 1)  as pct_pre,
    coalesce(a.n_post, 0)                                   as n_post,
    coalesce(a.n_true_post, 0)                              as n_true_post,
    round(100.0 * a.n_true_post / nullif(a.n_post, 0), 1)  as pct_post,
    round(100.0 * a.n_true_post / nullif(a.n_post, 0)
        - 100.0 * a.n_true_pre  / nullif(a.n_pre,  0), 1)  as delta_pp
from labels l
left join training_meta m using (training)
left join agg a using (training, question_no)
order by l.training, l.question_no