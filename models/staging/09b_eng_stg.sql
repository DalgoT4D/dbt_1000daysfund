{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    quoting={'identifier': true}
) }}

select
    pre_post                                                as survey_stage,
    nik                                                     as national_id_number,
    nama                                                    as respondent_name,
    usia::numeric                                           as respondent_age,
    score,
    max_score,
    jenis_kelamin                                           as gender,
    pendidikan_terakhir                                     as highest_education_level,
    nomor_hp_wa                                             as whatsapp_phone_number,
    provinsi                                                as province,
    kabupaten                                               as regency_or_city,
    kecamatan                                               as district,
    desa                                                    as village,
    puskesmas_pengampu                                      as supervising_primary_health_center_name,
    nama_posyandu_binaan                                    as assisted_integrated_health_post_name,
    submission_timestamp                                    as response_timestamp,

    -- Question 1: Which pregnant women are targeted for the case management support program?
case when q1_sasaran_dampingan_ibu_hamil = 'Bumil KEK & Anemia' then 'Pregnant women with chronic energy deficiency (CED) & anemia'
     when q1_sasaran_dampingan_ibu_hamil = 'Bumil Hipertensi & atau KEK' then 'Pregnant women with hypertension and/or chronic energy deficiency (CED)'
     when q1_sasaran_dampingan_ibu_hamil = 'Bumil Obesitas & KEK' then 'Pregnant women with obesity & chronic energy deficiency (CED)'
     when q1_sasaran_dampingan_ibu_hamil = 'Bumil Anemia & obesitas' then 'Pregnant women with anemia & obesity'
     else q1_sasaran_dampingan_ibu_hamil end as q1_target_pregnant_women_for_case_management_support,


    -- Question 2: Why should a pregnant woman's blood pressure be monitored?
case when q2_alasan_pemantauan_tekanan_darah_ibu_hamil = 'Agar bisa mendeteksi sejak dini masalah tekanan darah tinggi' then 'To detect high blood pressure issues early' 
when q2_alasan_pemantauan_tekanan_darah_ibu_hamil = 'Supaya tidak terkena anemia' then 'To prevent anemia' 
when q2_alasan_pemantauan_tekanan_darah_ibu_hamil = 'Mengurangi resiko bayi BBLR dan masalah kehamilan lainnya' then 'To reduce the risk of low birth weight (LBW) babies and other pregnancy complications' 
when q2_alasan_pemantauan_tekanan_darah_ibu_hamil = 'A & B benar' then 'A & B are correct' 
else q2_alasan_pemantauan_tekanan_darah_ibu_hamil end as q2_reason_to_monitor_pregnancy_blood_pressure,

    -- Question 3: What is the main advice given to pregnant women to avoid hypertension?
case 
    when q3_saran_utama_agar_terhindar_dari_hipertensi = 'Makan makanan yang mengandung kalsium dan suplemen kalsium' then 'Eat foods rich in calcium and take calcium supplements'
    when q3_saran_utama_agar_terhindar_dari_hipertensi = 'Istirahat sebanyak mungkin' then 'Get as much rest as possible'
    when q3_saran_utama_agar_terhindar_dari_hipertensi = 'Minum tablet tambah darah' then 'Take iron supplements (blood supplement tablets)'
    when q3_saran_utama_agar_terhindar_dari_hipertensi = 'Kurangi konsumsi garam'  then 'Reduce salt intake'
            else q3_saran_utama_agar_terhindar_dari_hipertensi end as q3_main_advice_to_prevent_hypertension_in_pregnancy,

    -- Question 4: Response captured for a pregnant participant with high systolic blood pressure.
case 
    when q4_tindakan_tekanan_sistolik_tinggi = 'Tekanan darah sangat rendah' then 'Very low blood pressure'
    when q4_tindakan_tekanan_sistolik_tinggi = 'Tekanan darah tinggi'  then 'High blood pressure'
    when q4_tindakan_tekanan_sistolik_tinggi = 'Tekanan darah normal' then 'Normal blood pressure'
    when q4_tindakan_tekanan_sistolik_tinggi = 'Tekanan darah rendah' then 'Low blood pressure'   
    else q4_tindakan_tekanan_sistolik_tinggi
end as q4_action_for_high_systolic_blood_pressure,

    -- Question 5: Response captured for a pregnant participant with blood pressure above 140.
   case 
    when q5_tindakan_tekanan_darah_atas_140 = 'Tekanan darah tinggi' then 'High blood pressure'
    when q5_tindakan_tekanan_darah_atas_140 = 'Tekanan darah normal' then 'Normal blood pressure'
    when q5_tindakan_tekanan_darah_atas_140 = 'Tekanan darah sangat tinggi' then 'Very high blood pressure'
    when q5_tindakan_tekanan_darah_atas_140 = 'Tekanan darah rendah' then 'Low blood pressure'
    else q5_tindakan_tekanan_darah_atas_140
end as q5_action_for_blood_pressure_above_140,

    -- Question 6: Response captured for a pregnant participant with blood pressure above 120.
case 
    when q6_tindakan_tekanan_darah_atas_120 = 'Tekanan darah tinggi' then 'High blood pressure'
    when q6_tindakan_tekanan_darah_atas_120 = 'Tekanan darah normal' then 'Normal blood pressure'
    when q6_tindakan_tekanan_darah_atas_120 = 'Tekanan darah sangat tinggi' then 'Very high blood pressure'
    when q6_tindakan_tekanan_darah_atas_120 = 'Tekanan darah rendah' then 'Low blood pressure'
    else q6_tindakan_tekanan_darah_atas_120
end as q6_action_for_blood_pressure_above_120,
   
    -- Question 7: When is a pregnant woman considered at risk of high blood pressure?
   case 
    when q7_kriteria_risiko_darah_tinggi = 'Tekanan darah di bawah 140/90' then 'Blood pressure below 140/90'
    when q7_kriteria_risiko_darah_tinggi = 'Tekanan darah lebih besar atau sama dengan  140/90' then 'Blood pressure greater than or equal to 140/90'
    when q7_kriteria_risiko_darah_tinggi = 'Tekanan darah di atas 130 / 80' then 'Blood pressure above 130/80'
    when q7_kriteria_risiko_darah_tinggi = 'Tekanan darah di bawah 130/90' then 'Blood pressure below 130/90'
    else q7_kriteria_risiko_darah_tinggi
end as q7_high_blood_pressure_risk_definition,
   
    -- Question 8: What risks can occur when a pregnant woman experiences chronic energy deficiency (KEK)?
    case 
    when q8_risiko_ibu_hamil_kek = 'Ibu hamil terlihat kurus' then 'Pregnant woman appears underweight'
    when q8_risiko_ibu_hamil_kek = 'Bayi  lahir cukup umur' then 'Baby is born at full term'
    when q8_risiko_ibu_hamil_kek = 'bayi lahir dengan berat badan di atas 2500 gram' then 'Baby is born with a weight above 2500 grams'
    when q8_risiko_ibu_hamil_kek = 'Ada resiko BBLR dan potensi stunting' then 'There is a risk of low birth weight (LBW) and potential stunting'
    else q8_risiko_ibu_hamil_kek
end as q8_risks_of_maternal_chronic_energy_deficiency,
   
   
    -- Question 9: How do you monitor blood pressure?
   case 
    when q9_cara_memantau_tekanan_darah = 'Banyak konsumsi sayur dan buah-buahan' then 'Consume plenty of vegetables and fruits'
    when q9_cara_memantau_tekanan_darah = 'Hindari makanan yang asin' then 'Avoid salty foods'
    when q9_cara_memantau_tekanan_darah = 'Belum tahu' then 'Do not know yet'
    when q9_cara_memantau_tekanan_darah = 'Pantau rutin tekanan darah, berkala di Posyandu' then 'Regularly monitor blood pressure at Posyandu'
    else q9_cara_memantau_tekanan_darah
end as q9_how_to_monitor_blood_pressure,
   
    -- Question 10: What should be done to keep a pregnant woman's blood pressure healthy?
   case 
    when q10_menjaga_tekanan_darah_ibu = 'Hindari makanan dan minuman manis' then 'Avoid sweet foods and drinks'
    when q10_menjaga_tekanan_darah_ibu = 'Hindari kenaikan berat badan berlebihan' then 'Avoid excessive weight gain'
    when q10_menjaga_tekanan_darah_ibu = 'Semua benar' then 'All are correct'
    when q10_menjaga_tekanan_darah_ibu = 'Minum suplemen kalsium' then 'Take calcium supplements'
    else q10_menjaga_tekanan_darah_ibu
end as q10_how_to_keep_maternal_blood_pressure_healthy,
   
    -- Question 11: How many times should a mother have pregnancy checkups at a health facility?
   case 
    when q11_frekuensi_pemeriksaan_kehamilan_di_fasilitas_kesehatan = 'Tidak tahu' then 'Do not know'
    when q11_frekuensi_pemeriksaan_kehamilan_di_fasilitas_kesehatan = '6 kali' then '6 times'
    when q11_frekuensi_pemeriksaan_kehamilan_di_fasilitas_kesehatan = '3 kali' then '3 times'
    when q11_frekuensi_pemeriksaan_kehamilan_di_fasilitas_kesehatan = '4 kali' then '4 times'
    else q11_frekuensi_pemeriksaan_kehamilan_di_fasilitas_kesehatan
end as q11_frequency_of_pregnancy_checkups_at_health_facilities,
   
    -- Question 12: What monitoring is needed for pregnant women targeted for hypertension support?
   case 
    when q12_monitoring_ibu_hamil_hipertensi = 'Kenaikan berat badan' then 'Weight gain'
    when q12_monitoring_ibu_hamil_hipertensi = 'A & C benar' then 'A & C are correct'
    when q12_monitoring_ibu_hamil_hipertensi = 'Asupan ibu hamil bervariasi, mengandung kalsium dan suplementasi kalsium' then 'Pregnant woman’s diet is varied, includes calcium and calcium supplementation'
    when q12_monitoring_ibu_hamil_hipertensi = 'Ukuran lila berkala agar Lila tidak kurang dari 23,5 cm' then 'Regular MUAC measurement to ensure it is not less than 23.5 cm'
    else q12_monitoring_ibu_hamil_hipertensi
end as q12_monitoring_for_pregnant_women_with_hypertension,
   
    -- Question 13: Which children under two are targeted for the support program?
   case 
    when q13_sasaran_dampingan_baduta = 'Anak dengan berat badan sangat kurang dan gizi kurang' then 'Children with severely underweight and undernutrition'
    when q13_sasaran_dampingan_baduta = 'Anak dengan perlambatan berat badan dan tinggi badan pendek ( dibawah -2SD)' then 'Children with slowed weight gain and short height (below -2SD)'
    when q13_sasaran_dampingan_baduta = 'Anak dengan berat badan sangat kurang dan sangat pendek' then 'Children with severely underweight and very short stature'
    when q13_sasaran_dampingan_baduta = 'Anak dengan status gizi buruk dan berat badan sangat kurang' then 'Children with severe malnutrition and severely underweight'
    else q13_sasaran_dampingan_baduta
end as q13_target_children_under_two_for_support,
   
    -- Question 14: Why must slowed weight gain be monitored?
   case 
    when q14_alasan_memantau_perlambatan_berat_badan = 'Perlambatan berat badan jika berlangsung terus menerus akan menyebabkan stunting' then 'Continuous slowed weight gain can lead to stunting'
    when q14_alasan_memantau_perlambatan_berat_badan = 'Berat badan paling mudah dipantau' then 'Weight is the easiest to monitor'
    when q14_alasan_memantau_perlambatan_berat_badan = 'Mendeteksi dini agar tahu adanya gangguan pertumbuhan' then 'Early detection to identify growth disorders'
    when q14_alasan_memantau_perlambatan_berat_badan = 'Semua benar' then 'All are correct'
    else q14_alasan_memantau_perlambatan_berat_badan
end as q14_why_slowed_weight_gain_must_be_monitored,
   
    -- Question 15: In the case management program, who should receive support for growth slowing?
    case 
    when q15_sasaran_dampingan_perlambatan_pertumbuhan = 'Kurva pada KMS  yang garisnya naik' then 'Growth curve on KMS that is increasing'
    when q15_sasaran_dampingan_perlambatan_pertumbuhan = 'Kurva melandai atau turun pada 1x pengukuran' then 'Curve flattens or declines in one measurement'
    when q15_sasaran_dampingan_perlambatan_pertumbuhan = 'Kenaikan kurang dari KBM sebanyak 2 kali' then 'Weight gain below KBM twice'
    when q15_sasaran_dampingan_perlambatan_pertumbuhan = 'Kenaikan kurang dari KBM' then 'Weight gain below KBM'
    else q15_sasaran_dampingan_perlambatan_pertumbuhan
end as q15_target_for_growth_slowing_support,
   
   
    -- Question 16: If a child's weight gain is not aligned with KBM, what counseling should be given?
   case 
    when q16_penyuluhan_saat_kenaikan_bb_tidak_sesuai_kbm = 'Berikan oralit atau zinc jika bayi diare' then 'Give ORS or zinc if the baby has diarrhea'
    when q16_penyuluhan_saat_kenaikan_bb_tidak_sesuai_kbm = 'Berikan MPASI lebih cepat, dan banyak sayuran' then 'Introduce complementary feeding earlier and provide plenty of vegetables'
    when q16_penyuluhan_saat_kenaikan_bb_tidak_sesuai_kbm = 'Berikan susu formula atas rekomendasi dokter' then 'Provide formula milk based on doctor’s recommendation'
    when q16_penyuluhan_saat_kenaikan_bb_tidak_sesuai_kbm = 'Berikan ASI saja sesuai kemauan bayi, perhatikan posisi & pelekatan. Serta pantau kenaikan badan tiap bulan.' then 'Provide exclusive breastfeeding on demand, ensure proper positioning and latch, and monitor weight gain monthly'
    else q16_penyuluhan_saat_kenaikan_bb_tidak_sesuai_kbm
end as q16_counseling_when_weight_gain_is_off_kbm,
   
    -- Question 17: If weight gain is not aligned with KBM, what is the main counseling topic?
   case 
    when q17_penyuluhan_utama_saat_kenaikan_bb_tidak_sesuai_kbm = 'Berikan susu formula atas rekomendasi dokter' then 'Provide formula milk based on doctor’s recommendation'
    when q17_penyuluhan_utama_saat_kenaikan_bb_tidak_sesuai_kbm = 'Ajak untuk periksa ke tenaga kesehatan jika ada infeksi / penyakit lainnya' then 'Encourage visiting a health worker if there is an infection or other illness'
    when q17_penyuluhan_utama_saat_kenaikan_bb_tidak_sesuai_kbm = 'Tetap berikan ASI' then 'Continue breastfeeding'
    when q17_penyuluhan_utama_saat_kenaikan_bb_tidak_sesuai_kbm = 'Utamakan MPASI utamakan protein hewani seperti telur, ayam, ikan, daging dan juga lemak' then 'Prioritize complementary feeding, especially animal protein such as eggs, chicken, fish, meat, and fats'
    else q17_penyuluhan_utama_saat_kenaikan_bb_tidak_sesuai_kbm
end as q17_main_counseling_when_weight_gain_is_off_kbm, 

    -- Question 18: What length gain is expected during the first year after birth?
   case 
    when q18_pertambahan_panjang_badan_tahun_pertama = '23-27 cm' then '23–27 cm'
    when q18_pertambahan_panjang_badan_tahun_pertama = '10-14 cm' then '10–14 cm'
    when q18_pertambahan_panjang_badan_tahun_pertama = '27-30 cm' then '27–30 cm'
    when q18_pertambahan_panjang_badan_tahun_pertama = 'Belum tahu' then 'Do not know yet'
    else q18_pertambahan_panjang_badan_tahun_pertama
end as q18_length_gain_in_first_year_after_birth,
   
    -- Question 19: How can toddler growth be monitored?
    case 
    when q19_cara_memantau_pertumbuhan_balita = 'Semua benar' then 'All are correct'
    when q19_cara_memantau_pertumbuhan_balita = 'Kader mengajak ibu balita datang ke posyandu' then 'Cadres encourage mothers to bring toddlers to Posyandu'
    when q19_cara_memantau_pertumbuhan_balita = 'Melakukan kunjungan rumah kepada sasaran' then 'Conduct home visits to beneficiaries'
    when q19_cara_memantau_pertumbuhan_balita = 'Kader rutin mengisi (plotting) pada grafik KMS' then 'Cadres regularly plot data on the KMS growth chart'
    else q19_cara_memantau_pertumbuhan_balita
end as q19_how_to_monitor_toddler_growth,
   
    -- Question 20: What monitoring is needed for children under two with growth slowing?
case 
    when q20_monitoring_baduta_perlambatan_pertumbuhan = 'Memantau kenaikan berat badan sesuai KBM' then 'Monitor weight gain according to KBM'
    when q20_monitoring_baduta_perlambatan_pertumbuhan = 'Merujuk ke Puskesmas' then 'Refer to the primary health center (Puskesmas)'
    when q20_monitoring_baduta_perlambatan_pertumbuhan = 'Semua benar' then 'All are correct'
    when q20_monitoring_baduta_perlambatan_pertumbuhan = 'Memperhatikan asupan pemberian makan yang benar' then 'Ensure appropriate feeding practices'
    else q20_monitoring_baduta_perlambatan_pertumbuhan
end as q20_monitoring_for_children_with_growth_slowing, 

    training_strengths                                      as what_went_well_in_training,
    training_satisfaction                                   as training_satisfaction,
    training_improvements                                   as training_improvements_needed,
    confidence_to_apply_knowledge_or_skills                 as confidence_to_apply_knowledge_or_skills

from {{ ref('09b_stg') }}
