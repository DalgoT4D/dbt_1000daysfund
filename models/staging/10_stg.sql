{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true}, tags=["10_stg", "staging"] 
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
        
        -- Knowledge questions
        "1__Apakah_manfaat_ASI_bagi_bayi_"                                as q1_raw,
        "2__Bagaimana_prinsip_ASI_"                                       as q2_raw,
        "3__Dalam_menggunakan_lembar_ASI_mengenai_tanda_kecukupan_ASI__t" as q3_raw,
        "4__Bagaimana_pemberian_ASI_pada_bayi_0___6_bulan_saat_mengalami" as q4_raw,
        "5___ASI_yang_terbaik_adalah_yang_keluarnya_terakhir__karena_bis" as q5_raw,
        "6__Ketika_Ibu_sudah_bisa_memulai_masa_MPASI_untuk_anaknya__peny" as q6_raw,
        "7__Saat_anak_sudah_memasuki_usia_9_bulan__maka_pemberian_makan_" as q7_raw,
        "8__Di_grafik_berat_badan_anak_yang_ada_di_dalam_buku_KIA__terda" as q8_raw,
        "9__Berat_badan_Billy_bulan_lalu_4400_gr__angka_KBM_bulan_ini_ad" as q9_raw,
        "10__Rafika_berusia_10_bulan_menyukai_makanan_bubur_instan_dan_b" as q10_raw,
        "11__Ibu_Imelda_saat_datang_ke_posyandu_mengatakan_sudah_memulai" as q11_raw,
        
        -- Nulls for pre-training rows
        null::varchar                                                     as training_strengths_raw,
        null::varchar                                                     as training_satisfaction_raw,
        null::varchar                                                     as training_improvements_raw,
        null::varchar                                                     as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '10_pre') }}

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
        
        -- Knowledge questions
        "1__Apakah_manfaat_ASI_bagi_bayi_"                                as q1_raw,
        "2__Bagaimana_prinsip_ASI_"                                       as q2_raw,
        "3__Dalam_menggunakan_lembar_ASI_mengenai_tanda_kecukupan_ASI__t" as q3_raw,
        "4__Bagaimana_pemberian_ASI_pada_bayi_0___6_bulan_saat_mengalami" as q4_raw,
        "5___ASI_yang_terbaik_adalah_yang_keluarnya_terakhir__karena_bis" as q5_raw,
        "6__Ketika_Ibu_sudah_bisa_memulai_masa_MPASI_untuk_anaknya__peny" as q6_raw,
        "7__Saat_anak_sudah_memasuki_usia_9_bulan__maka_pemberian_makan_" as q7_raw,
        "8__Di_grafik_berat_badan_anak_yang_ada_di_dalam_buku_KIA__terda" as q8_raw,
        "9__Berat_badan_Billy_bulan_lalu_4400_gr__angka_KBM_bulan_ini_ad" as q9_raw,
        "10__Rafika_berusia_10_bulan_menyukai_makanan_bubur_instan_dan_b" as q10_raw,
        "11__Ibu_Imelda_saat_datang_ke_posyandu_mengatakan_sudah_memulai" as q11_raw,
        
        -- Post-training feedback
        "Apa_yang_sudah_baik_dari_pelatihan_hari_ini_"                    as training_strengths_raw,
        "Sejauh_mana_kepuasan_yang_Bapak_Ibu_berikan_terhadap_pelatihan_" as training_satisfaction_raw,
        "Perbaikan_seperti_apa_yang_perlu_kami_lakukan_untuk_pelatihan_k" as training_improvements_raw,
        "Saya_merasa_percaya_diri_untuk_menerapkan_pengetahuan_dan_atau_" as confidence_to_apply_knowledge_or_skills_raw

    from {{ source('raw_sheets', '10_post') }}

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
    nullif(btrim(nama_posyandu_binaan_raw), '')                                      as nama_posyandu_binaan,
    nullif(btrim(submission_timestamp_raw), '')                                      as submission_timestamp,

    -- Knowledge questions
    nullif(btrim(q1_raw), '')                                                        as q1_manfaat_asi_bagi_bayi,
    nullif(btrim(q2_raw), '')                                                        as q2_prinsip_asi,
    nullif(btrim(q3_raw), '')                                                        as q3_tanda_kecukupan_asi,
    nullif(btrim(q4_raw), '')                                                        as q4_pemberian_asi_0_6_bulan_kondisi_tertentu,
    nullif(btrim(q5_raw), '')                                                        as q5_asi_terbaik_keluar_terakhir,
    nullif(btrim(q6_raw), '')                                                        as q6_memulai_masa_mpasi,
    nullif(btrim(q7_raw), '')                                                        as q7_pemberian_makan_usia_9_bulan,
    nullif(btrim(q8_raw), '')                                                        as q8_grafik_berat_badan_buku_kia,
    nullif(btrim(q9_raw), '')                                                        as q9_kbm_berat_badan_billy,
    nullif(btrim(q10_raw), '')                                                       as q10_rafika_10_bulan_makanan_instan,
    nullif(btrim(q11_raw), '')                                                       as q11_ibu_imelda_memulai_mpasi,

    -- Post-training feedback
    nullif(btrim(training_strengths_raw), '')                                        as training_strengths,
    nullif(btrim(training_satisfaction_raw), '')                                     as training_satisfaction,
    nullif(btrim(training_improvements_raw), '')                                     as training_improvements,
    nullif(btrim(confidence_to_apply_knowledge_or_skills_raw), '')                   as confidence_to_apply_knowledge_or_skills

from combined_source