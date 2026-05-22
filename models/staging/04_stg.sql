{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true}, tags=["04_stg", "staging"] 
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
        "Nama_Posyandu_Binaan"                                            as nama_posyandu_raw,
        "Timestamp"                                                       as submission_timestamp_raw,
        
        -- Knowledge questions
        "1__Apa_itu_STUNTING_"                                            as q1_raw,
        "2___Apa_bahaya_utama_STUNTING_"                                  as q2_raw,
        "3__Yang_merupakan_manfaat_Tablet_Tambah_Darah__TTD__adalah_"     as q3_raw,
        "4__Efek_samping_konsumsi_Tablet_Tambah_Darah_adalah__"           as q4_raw,
        "5__Apa_minuman_atau_makanan_yang_perlu_dihindari_bersamaan_deng" as q5_raw,
        "6__Bahaya_dari_kekurangan_zat_besi_untuk_ibu_hamil_adalah_"      as q6_raw,
        "7__Kapan_saja_3_waktu_penting_untuk_cuci_tangan_pakai_sabun__"   as q7_raw,
        "8__Mana_pernyataan_yang_paling_tepat_terkait_ASI_eksklusif_"     as q8_raw,
        "9_Dari_pilihan_berikut_ini__manakah_makanan_perlu_diutamakan_ba" as q9_raw,
        "10__Yang_bukan_manfaat_pemberian_Vitamin_A_bagi_anak_adalah_"    as q10_raw,
        "11__Kapan_periode_1000_Hari_Pertama_Kehidupan__"                 as q11_raw,
        "12__Mengapa_periode_1000_hari_pertama_kehidupan_itu_penting__"   as q12_raw,
        "13__Poster_Pintar_adalah_sebuah_alat_untuk_menjelaskan_STUNTING" as q13_raw,
        "14__Cara_yang_benar_dalam_menempelkan_Poster_Pintar_adalah___"   as q14_raw,
        "15__Mana_yang_merupakan_salah_satu_strategi_komunikasi_dalam_pe" as q15_raw,
        "16__Yang_bukan_merupakan_teknik_konseling_kepada_sasaran__yaitu" as q16_raw,
        
        -- Nulls for post-training rows
        null::varchar                                                     as training_strengths_raw,
        null::varchar                                                     as training_satisfaction_raw,
        null::varchar                                                     as training_improvements_raw,
        null::varchar                                                     as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '04_pre') }}

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
        "Puskesmas_pengampu"                                              as puskesmas_pengampu_raw,
        "Nama_Posyandu"                                                   as nama_posyandu_raw,
        "Timestamp"                                                       as submission_timestamp_raw,
        
        -- Knowledge questions
        "1__Apa_itu_STUNTING_"                                            as q1_raw,
        "2__Apa_bahaya_utama_STUNTING_"                                   as q2_raw,
        "3__Yang_merupakan_manfaat_Tablet_Tambah_Darah__TTD__adalah_"     as q3_raw,
        "4__Efek_samping_konsumsi_Tablet_Tambah_Darah_adalah__"           as q4_raw,
        "5__Apa_minuman_atau_makanan_yang_perlu_dihindari_bersamaan_deng" as q5_raw,
        "6__Bahaya_dari_kekurangan_zat_besi_untuk_ibu_hamil_adalah_"      as q6_raw,
        "7__Kapan_saja_3_waktu_penting_untuk_cuci_tangan_pakai_sabun__"   as q7_raw,
        "8__Mana_pernyataan_yang_paling_tepat_terkait_ASI_eksklusif_"     as q8_raw,
        "9__Dari_pilihan_berikut_ini__manakah_makanan_perlu_diutamakan_b" as q9_raw,
        "10__Yang_bukan_manfaat_pemberian_Vitamin_A_bagi_anak_adalah_"    as q10_raw,
        "11__Kapan_periode_1000_Hari_Pertama_Kehidupan__"                 as q11_raw,
        "12__Mengapa_periode_1000_hari_pertama_kehidupan_itu_penting__"   as q12_raw,
        "13__Poster_Pintar_adalah_sebuah_alat_untuk_menjelaskan_STUNTING" as q13_raw,
        "14__Cara_yang_benar_dalam_menempelkan_Poster_Pintar_adalah___"   as q14_raw,
        "15__Mana_yang_merupakan_salah_satu_strategi_komunikasi_dalam_pe" as q15_raw,
        "16__Yang_bukan_merupakan_teknik_konseling_kepada_sasaran__yaitu" as q16_raw,
        
        -- Post-training feedback
        "Apa_yang_sudah_baik_dari_pelatihan_hari_ini_"                    as training_strengths_raw,
        "Sejauh_mana_kepuasan_yang_Bapak_Ibu_berikan_terhadap_pelatihan_" as training_satisfaction_raw,
        "Perbaikan_seperti_apa_yang_perlu_kami_lakukan_untuk_pelatihan_k" as training_improvements_raw,
        "Saya_merasa_percaya_diri_untuk_menerapkan_pengetahuan_dan_atau_" as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '04_post') }}

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

    -- Location and assignment details
    nullif(btrim(provinsi_raw), '')                                                  as provinsi,
    nullif(btrim(kabupaten_raw), '')                                                 as kabupaten,
    nullif(btrim(kecamatan_raw), '')                                                 as kecamatan,
    nullif(btrim(desa_raw), '')                                                      as desa,
    nullif(btrim(puskesmas_pengampu_raw), '')                                        as puskesmas_pengampu,
    nullif(btrim(nama_posyandu_raw), '')                                             as nama_posyandu,
    nullif(btrim(submission_timestamp_raw), '')                                      as submission_timestamp,

    -- Knowledge questions
    nullif(btrim(q1_raw), '')                                                        as q1_apa_itu_stunting,
    nullif(btrim(q2_raw), '')                                                        as q2_bahaya_utama_stunting,
    nullif(btrim(q3_raw), '')                                                        as q3_manfaat_ttd,
    nullif(btrim(q4_raw), '')                                                        as q4_efek_samping_ttd,
    nullif(btrim(q5_raw), '')                                                        as q5_dihindari_bersamaan_ttd,
    nullif(btrim(q6_raw), '')                                                        as q6_bahaya_kurang_zat_besi,
    nullif(btrim(q7_raw), '')                                                        as q7_waktu_cuci_tangan,
    nullif(btrim(q8_raw), '')                                                        as q8_pernyataan_asi_eksklusif,
    nullif(btrim(q9_raw), '')                                                        as q9_makanan_perlu_diutamakan,
    nullif(btrim(q10_raw), '')                                                       as q10_bukan_manfaat_vitamin_a,
    nullif(btrim(q11_raw), '')                                                       as q11_kapan_periode_1000_hpk,
    nullif(btrim(q12_raw), '')                                                       as q12_mengapa_1000_hpk_penting,
    nullif(btrim(q13_raw), '')                                                       as q13_poster_pintar_adalah_alat,
    nullif(btrim(q14_raw), '')                                                       as q14_cara_menempelkan_poster_pintar,
    nullif(btrim(q15_raw), '')                                                       as q15_strategi_komunikasi_perubahan_perilaku,
    nullif(btrim(q16_raw), '')                                                       as q16_bukan_teknik_konseling,

    -- Post-training feedback
    nullif(btrim(training_strengths_raw), '')                                        as training_strengths,
    nullif(btrim(training_satisfaction_raw), '')                                     as training_satisfaction,
    nullif(btrim(training_improvements_raw), '')                                     as training_improvements,
    nullif(btrim(confidence_to_apply_knowledge_or_skills_raw), '')                   as confidence_to_apply_knowledge_or_skills

from combined_source