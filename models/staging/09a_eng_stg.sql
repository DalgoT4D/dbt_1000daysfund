{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true}
) }}

select
    pre_post                                               as survey_stage,
    nik                                                    as national_id_number,
    nama                                                   as respondent_name,
    usia::numeric                                          as respondent_age,
    peran                                                  as respondent_role,
    score,
    max_score,
    jenis_kelamin                                          as gender,
    pendidikan_terakhir                                    as highest_education_level,
    nomor_hp_wa                                            as whatsapp_phone_number,
    provinsi                                               as province,
    kabupaten                                              as regency_or_city,
    kecamatan                                              as district,
    desa                                                   as village,
    puskesmas_pengampu                                     as supervising_primary_health_center_name,
    nama_posyandu_binaan                                   as assisted_integrated_health_post_name,
    submission_timestamp                                   as response_timestamp,

    -- Question 1: Which pregnant women are targeted for the case management support program?
    q1_sasaran_dampingan_ibu_hamil                         as q1_target_pregnant_women_for_case_management_support,
    -- Question 2: Why should a pregnant woman's blood pressure be monitored?
    q2_alasan_pemantauan_tekanan_darah_ibu_hamil           as q2_reason_to_monitor_pregnancy_blood_pressure,
    -- Question 3: What can cause hypertension to increase during pregnancy?
    q3_penyebab_hipertensi_saat_hamil                      as q3_cause_of_hypertension_in_pregnancy,
    -- Question 4: Which option is not recommended to help a mother avoid hypertension?
    q4_bukan_saran_pencegahan_hipertensi                   as q4_not_a_hypertension_prevention_recommendation,
    -- Question 5: Response captured for a pregnant participant with high systolic blood pressure.
    q5_tindakan_tekanan_sistolik_tinggi                    as q5_action_for_high_systolic_blood_pressure,
    -- Question 6: Response captured for a pregnant participant with blood pressure above 140.
    q6_tindakan_tekanan_darah_atas_140                     as q6_action_for_blood_pressure_above_140,
    -- Question 7: Response captured for a pregnant participant with blood pressure above 120.
    q7_tindakan_tekanan_darah_atas_120                     as q7_action_for_blood_pressure_above_120,
    -- Question 8: When is a pregnant woman considered at risk of high blood pressure?
    q8_kriteria_risiko_darah_tinggi                        as q8_high_blood_pressure_risk_definition,
    -- Question 9: What risks can occur when a pregnant woman experiences chronic energy deficiency (KEK)?
    q9_risiko_ibu_hamil_kek                                as q9_risks_of_maternal_chronic_energy_deficiency,
    -- Question 10: How do you monitor blood pressure?
    q10_cara_memantau_tekanan_darah                        as q10_how_to_monitor_blood_pressure,
    -- Question 11: What should be done to keep a pregnant woman's blood pressure healthy?
    q11_menjaga_tekanan_darah_ibu                          as q11_how_to_keep_maternal_blood_pressure_healthy,
    -- Question 12: Which option is not a benefit of iron supplement tablets (TTD)?
    q12_bukan_manfaat_ttd                                  as q12_not_a_benefit_of_iron_tablets,
    -- Question 13: Which option is not recommended advice for a pregnant woman with chronic energy deficiency (KEK)?
    q13_bukan_saran_untuk_ibu_kek                          as q13_not_advice_for_maternal_chronic_energy_deficiency,
    -- Question 14: What are the benefits of calcium-rich foods or calcium supplements?
    q14_manfaat_kalsium                                    as q14_benefits_of_calcium_foods_or_supplements,
    -- Question 15: Which information or counseling can be given to a pregnant woman with hypertension?
    q15_penyuluhan_untuk_ibu_hipertensi                    as q15_counseling_for_pregnant_women_with_hypertension,
    -- Question 16: What monitoring is needed for pregnant women targeted for hypertension support?
    q16_monitoring_ibu_hamil_hipertensi                    as q16_monitoring_for_pregnant_women_with_hypertension,
    -- Question 17: Which children under two are targeted for the support program?
    q17_sasaran_dampingan_baduta                           as q17_target_children_under_two_for_support,
    -- Question 18: Why must slowed weight gain be monitored?
    q18_alasan_memantau_perlambatan_berat_badan            as q18_why_slowed_weight_gain_must_be_monitored,
    -- Question 19: In the case management program, who should receive support for growth slowing?
    q19_sasaran_dampingan_perlambatan_pertumbuhan          as q19_target_for_growth_slowing_support,
    -- Question 20: If a child's weight gain is not aligned with KBM, what counseling should be given?
    q20_penyuluhan_saat_kenaikan_bb_tidak_sesuai_kbm       as q20_counseling_when_weight_gain_is_off_kbm,
    -- Question 21: If weight gain is not aligned with KBM, what is the main counseling topic?
    q21_penyuluhan_utama_saat_kenaikan_bb_tidak_sesuai_kbm as q21_main_counseling_when_weight_gain_is_off_kbm,
    -- Question 22: Which growth status corresponds to weight that does not increase?
    q22_status_pertumbuhan_tidak_naik                      as q22_growth_status_for_weight_not_increasing,
    -- Question 23: Which option is not a cause of growth slowing?
    q23_bukan_penyebab_perlambatan_pertumbuhan             as q23_not_a_cause_of_growth_slowing,
    -- Question 24: What length gain is expected during the first year after birth?
    q24_pertambahan_panjang_badan_tahun_pertama            as q24_length_gain_in_first_year_after_birth,
    -- Question 25: How can toddler growth be monitored?
    q25_cara_memantau_pertumbuhan_balita                   as q25_how_to_monitor_toddler_growth,
    -- Question 26: What monitoring is needed for children under two with growth slowing?
    q26_monitoring_baduta_perlambatan_pertumbuhan          as q26_monitoring_for_children_with_growth_slowing,

    -- Post-training feedback
    training_strengths                                     as what_went_well_in_training,
    training_satisfaction                                  as training_satisfaction,
    training_improvements                                  as training_improvements_needed,
    confidence_to_apply_knowledge_or_skills                as confidence_to_apply_knowledge_or_skills

from {{ ref('09a_stg') }}
