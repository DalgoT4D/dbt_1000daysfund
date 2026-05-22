{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true}, tags=["06_stg", "staging"] 
) }}

with pre_source as (

    select
        'pre'::text                                                       as pre_post,
        "NIK"                                                             as nik_raw,
        "Nama_lengkap"                                                    as nama_raw,
        "Usia"                                                            as usia_raw,
        "Peran_anda"                                                      as peran_raw,
        "Score"                                                           as score_raw,
        "Jenis_kelamin"                                                   as jenis_kelamin_raw,
        "Pendidikan_terakhir"                                             as pendidikan_terakhir_raw,
        "Nomor_HP_Whatsapp"                                               as nomor_hp_wa_raw,
        "Email_Address"                                                   as email_address_raw,
        "Provinsi"                                                        as provinsi_raw,
        "Kabupaten"                                                       as kabupaten_raw,
        "Kecamatan"                                                       as kecamatan_raw,
        "Desa"                                                            as desa_raw,
        "Puskesmas_pengampu"                                              as puskesmas_pengampu_raw,
        "Asal_Posyandu"                                                   as nama_posyandu_raw,
        "Timestamp"                                                       as submission_timestamp_raw,
        
        -- Knowledge questions
        "1__Apa_yang_dapat_dilakukan_setelah_hari_buka_posyandu_"         as q1_raw,
        "2__Bagaimana_Langkah_pelayanan_hari_buka_posyandu_secara_beruru" as q2_raw,
        "3__Pada_langkah_penimbangan_dan_pengukuran_bagi_sasaran_bayi_ba" as q3_raw,
        "4__Pada_langkah_penyuluhan_kesehatan_bagi_sasaran_ibu_hamil__ya" as q4_raw,
        "5__Ketika_sasaran_bayi_di_bawah_dua_tahun__Baduta__dilakukan_pe" as q5_raw,
        "6__Ketika_melakukan_kunjungan_rumah__apa_saja_yang_harus_dilaku" as q6_raw,
        "7__Selama_berkunjung_ke_rumah__apa_langkah_yang_perlu_dilakukan" as q7_raw,
        "8__Pemantauan_ibu_hamil_KEK_di_Posyandu_meliputi___"             as q8_raw,
        "9__Anjuran_minum_tablet_tambah_darah_bagi_ibu_hamil_yang_paling" as q9_raw,
        "10__Perbedaan_pola_makan_ibu_hamil_dan_ibu_menyusui_dibandingka" as q10_raw,
        "11__Makanan_tambahan_yang_disarankan_untuk_penyuluhan_di_Posyan" as q11_raw,
        "12__Kader_Sasa_menemui_sasaran_baduta_yang_saat_pengukuran_di_P" as q12_raw,
        
        -- Pre-training expectations (Only in pre)
        "Apa_harapan_Anda_terhadap_pelatihan_hari_ini_"                   as training_expectations_raw,
        "Apa_yang_anda_khawatirkan_dari_mengikuti_pelatihan_hari_ini_"    as training_worries_raw,

        -- Nulls for post-training rows
        null::varchar                                                     as training_strengths_raw,
        null::varchar                                                     as training_satisfaction_raw,
        null::varchar                                                     as training_improvements_raw,
        null::varchar                                                     as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '06_pre') }}

),

post_source as (

    select
        'post'::text                                                      as pre_post,
        "NIK"                                                             as nik_raw,
        "Nama_lengkap"                                                    as nama_raw,
        "Usia"                                                            as usia_raw,
        "Peran_anda"                                                      as peran_raw,
        "Score"                                                           as score_raw,
        "Jenis_kelamin"                                                   as jenis_kelamin_raw,
        "Pendidikan_terakhir"                                             as pendidikan_terakhir_raw,
        "Nomor_HP_Whatsapp"                                               as nomor_hp_wa_raw,
        "Email_Address"                                                   as email_address_raw,
        "Provinsi"                                                        as provinsi_raw,
        "Kabupaten"                                                       as kabupaten_raw,
        "Kecamatan"                                                       as kecamatan_raw,
        "Desa"                                                            as desa_raw,
        "Puskesmas_pengampu"                                              as puskesmas_pengampu_raw,
        "Asal_Posyandu"                                                   as nama_posyandu_raw,
        "Timestamp"                                                       as submission_timestamp_raw,
        
        -- Knowledge questions
        "1__Apa_yang_dapat_dilakukan_setelah_hari_buka_posyandu_"         as q1_raw,
        "2__Bagaimana_Langkah_pelayanan_hari_buka_posyandu_secara_beruru" as q2_raw,
        "3__Pada_langkah_penimbangan_dan_pengukuran_bagi_sasaran_bayi_ba" as q3_raw,
        "4__Pada_langkah_penyuluhan_kesehatan_bagi_sasaran_ibu_hamil__ya" as q4_raw,
        "5__Ketika_sasaran_bayi_di_bawah_dua_tahun__Baduta__dilakukan_pe" as q5_raw,
        "6__Ketika_melakukan_kunjungan_rumah__apa_saja_yang_harus_dilaku" as q6_raw,
        "7__Selama_berkunjung_ke_rumah__apa_langkah_yang_perlu_dilakukan" as q7_raw,
        "8__Pemantauan_ibu_hamil_KEK_di_Posyandu_meliputi___"             as q8_raw,
        "9__Anjuran_minum_tablet_tambah_darah_bagi_ibu_hamil_yang_paling" as q9_raw,
        "10__Perbedaan_pola_makan_ibu_hamil_dan_ibu_menyusui_dibandingka" as q10_raw,
        "11__Makanan_tambahan_yang_disarankan_untuk_penyuluhan_di_Posyan" as q11_raw,
        "12__Kader_Sasa_menemui_sasaran_baduta_yang_saat_pengukuran_di_P" as q12_raw,
        
        -- Nulls for pre-training rows
        null::varchar                                                     as training_expectations_raw,
        null::varchar                                                     as training_worries_raw,

        -- Post-training feedback (Only in post)
        "Apa_yang_sudah_baik_dari_pelatihan_hari_ini_"                    as training_strengths_raw,
        "Sejauh_mana_kepuasan_yang_Bapak_Ibu_berikan_terhadap_pelatihan_" as training_satisfaction_raw,
        "Perbaikan_seperti_apa_yang_perlu_kami_lakukan_untuk_pelatihan_k" as training_improvements_raw,
        "Saya_merasa_percaya_diri_untuk_menerapkan_pengetahuan_dan_atau_" as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '06_post') }}

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
    nullif(btrim(q1_raw), '')                                                        as q1_setelah_hari_buka_posyandu,
    nullif(btrim(q2_raw), '')                                                        as q2_langkah_pelayanan_posyandu,
    nullif(btrim(q3_raw), '')                                                        as q3_penimbangan_pengukuran_bayi,
    nullif(btrim(q4_raw), '')                                                        as q4_penyuluhan_ibu_hamil,
    nullif(btrim(q5_raw), '')                                                        as q5_tindakan_sasaran_baduta,
    nullif(btrim(q6_raw), '')                                                        as q6_tindakan_kunjungan_rumah,
    nullif(btrim(q7_raw), '')                                                        as q7_langkah_kunjungan_rumah,
    nullif(btrim(q8_raw), '')                                                        as q8_pemantauan_ibu_hamil_kek,
    nullif(btrim(q9_raw), '')                                                        as q9_anjuran_minum_ttd,
    nullif(btrim(q10_raw), '')                                                       as q10_perbedaan_pola_makan_ibu_hamil_menyusui,
    nullif(btrim(q11_raw), '')                                                       as q11_makanan_tambahan_penyuluhan,
    nullif(btrim(q12_raw), '')                                                       as q12_kader_sasa_menemui_baduta,

    -- Pre-training expectations
    nullif(btrim(training_expectations_raw), '')                                     as training_expectations,
    nullif(btrim(training_worries_raw), '')                                          as training_worries,

    -- Post-training feedback
    nullif(btrim(training_strengths_raw), '')                                        as training_strengths,
    nullif(btrim(training_satisfaction_raw), '')                                     as training_satisfaction,
    nullif(btrim(training_improvements_raw), '')                                     as training_improvements,
    nullif(btrim(confidence_to_apply_knowledge_or_skills_raw), '')                   as confidence_to_apply_knowledge_or_skills

from combined_source