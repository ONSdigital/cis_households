update_output_name_maps = {
    "unioned_responses_update_map": {
        "cis_covid_vaccine_date": "covid_vaccine_date",
        "cis_covid_vaccine_date_1": "covid_vaccine_date1",
        "cis_covid_vaccine_date_2": "covid_vaccine_date2",
        "cis_covid_vaccine_date_3": "covid_vaccine_date3",
        "cis_covid_vaccine_date_4": "covid_vaccine_date4",
        "cis_covid_vaccine_type_1": "covid_vaccine_type1",
        "cis_covid_vaccine_type_2": "covid_vaccine_type2",
        "cis_covid_vaccine_type_3": "covid_vaccine_type3",
        "cis_covid_vaccine_type_4": "covid_vaccine_type4",
        "cis_covid_vaccine_type_other_1": "covid_vaccine_type_other1",
        "cis_covid_vaccine_type_other_2": "covid_vaccine_type_other2",
        "cis_covid_vaccine_type_other_3": "covid_vaccine_type_other3",
        "cis_covid_vaccine_type_other_4": "covid_vaccine_type_other4",
        "date_of_birth": "dob",
        "withdrawal_type": "withdrawn_type",
        "is_self_isolating_detailed": "self_isolating_v1",
        "consent_blood_samples_if_positive": "consent_blood_if_positive",
        "consent_blood_test": "consent_blood",
        "symptoms_since_last_visit_more_trouble_sleeping": "sympt_covid_trouble_sleeping",
        "symptoms_since_last_visit_noisy_breathing_wheezing": "sympt_covid_noisy_breathing",
        "consent_16_visits": "consent_v16",
        "contact_suspect_positive_covid_last_28_days": "contact_suspect_covid",
        "not_attended_reason": "not_attend_reason",
        "improved_visit_date": "actual_visit_date_time",
        "improved_visit_date_string": "actual_visit_date",
        "symptoms_last_7_days_any": "sympt_now_any",
        "think_had_covid_any_symptoms": "sympt_covid_any",
        "hospital_last_28_days_other_household_member": "contact_hospital",
        "care_home_last_28_days_other_household_member": "contact_carehome",
        "work_status_v1_raw": "orig_v1_work_status",
    }
}

output_name_map = {
    "blood_sample_received_date": "DateSamplesReceivedOxford",
    "able_to_take_blood": "able_to_take_blood",
    "other_antibody_test_since_last_visit": "covid_test_blood",
    "work_in_additional_paid_employment": "additional_paid_employment",
    "age_at_visit": "age_at_visit",
    "age_at_visit_raw": "age_at_visit",
    "age_group_5_year_intervals": "ageg",
    "age_group_7_intervals": "ageg_7",
    "age_group_5_intervals": "ageg_small",
    "age_group_over_16": "ageg_small2",
    "age_group_school_year": "ageg_sy",
    "any_symptoms_around_visit": "any_evidence_sympt_around",
    "any_symptoms_last_7_days_or_now": "any_evidence_sympt_now",
    "household_visit_status": "appointment_status",
    "approached_for_blood_samples": "approached_for_blood_samples",
    "blood_sample_arrayed_date_s_protein": "arrayed_ox_date",
    "blood_sample_arrayed_date_n_protein": "arrayed_ox_date_n",
    "antibody_assay_category_s_protein": "assay_category",
    "antibody_assay_category_n_protein": "assay_category_n",
    "antibody_test_result_value_s_protein": "assay_mabs",
    "antibody_test_result_value_n_protein": "assay_mabs_n",
    "assay_siemens_s_protein": "assay_siemens",
    "assay_siemens_n_protein": "assay_siemens_n",
    "antibody_test_tdi_result_value_s_protein": "assay_tdi",
    "antibody_test_tdi_result_value_n_protein": "assay_tdi_n",
    "blood_sample_barcode": "blood_barcode_cleaned",
    "blood_invite_past_positive_pcr_test": "blood_past_positive",
    "blood_taken": "blood_taken",
    "blood_sample_type": "blood_sample_type",
    "cis_area_name_20": "cis20_name",
    "cis_area_code_20": "cis20_samp",
    "cis_covid_vaccine_classification": "cis_class_vaccine",
    "cis_covid_vaccine_dose_1_datetime": "cis_vaccine_date1",
    "cis_covid_vaccine_dose_2_datetime": "cis_vaccine_date2",
    "cis_covid_vaccine_dose_1_type": "cis_vaccine_type1",
    "cis_covid_vaccine_dose_2_type": "cis_vaccine_type2",
    "combined_covid_vaccine_classification": "class_vaccine",
    "study_cohort": "cohort",
    "consent_summary": "consent",
    "consent_blood_samples_if_positiveyn": "consent_blood_if_positiveYN",
    "consent_use_of_surplus_blood_samples": "consent_blood_surplus",
    "consent_use_of_surplus_blood_samplesyn": "consent_blood_surplusYN",
    "consent_contact_extra_research": "consent_future_research",
    "consent_contact_extra_researchyn": "consent_future_researchYN",
    "consent_april_22": "consent_to_april_22",
    "consent_extend_study_under_16_b1_b3": "consent_to_extend_study_swab",
    "consent_finger_prick_a1_a3": "consent_to_finger_prick_a1_a3",
    "consent_fingerprick_blood_samples": "consent_to_fingerprick_blood",
    "consent_1_visit": "consent_v1",
    "consent_5_visits": "consent_v5",
    "contact_known_or_suspected_covid": "contact_any_covid",
    "contact_known_or_suspected_covid_latest_date": "contact_any_covid_date",
    "contact_known_or_suspected_covid_days_since": "contact_any_covid_dayssince",
    "contact_known_or_suspected_covid_days_since_group": "contact_any_covid_dayssinceg",
    "contact_known_or_suspected_covid_type": "contact_any_covid_type",
    "household_been_care_home_last_2_weeks": "contact_carehome",
    "face_covering_outside_of_home": "contact_face_covering",
    "face_covering_other_enclosed_places": "contact_face_covering_other",
    "face_covering_work": "contact_face_covering_workschool",
    "household_been_hospital_last_2_weeks": "contact_hospital",
    "contact_known_positive_covid_last_28_days": "contact_known_covid",
    "last_covid_contact_date": "contact_known_covid_date",
    "last_covid_contact_location": "contact_known_covid_type",
    "care_home_last_28_days_other_household_member": "contact_other_in_hh_carehome",
    "hospital_last_28_days_other_household_member": "contact_other_in_hh_hospital",
    "care_home_last_28_days": "contact_participant_carehome",
    "hospital_last_28_days": "contact_participant_hospital",
    "physical_contact_18_to_69_years": "contact_physical_18_69y",
    "physical_contact_under_18_years": "contact_physical_18y",
    "physical_contact_over_70_years": "contact_physical_70y",
    "social_distance_contact_18_to_69_years": "contact_social_dist_18_to_69",
    "social_distance_contact_under_18_years": "contact_social_dist_18y",
    "social_distance_contact_over_70_years": "contact_social_dist_70y",
    "contact_suspected_positive_covid_last_28_days": "contact_suspect_covid",
    "last_suspected_covid_contact_date": "contact_suspect_covid_date",
    "last_suspected_covid_contact_location": "contact_suspect_covid_type",
    "country": "country",
    "other_antibody_test_last_negative_date": "covid_test_blood_neg_last_date",
    "other_antibody_test_first_positive_date": "covid_test_blood_pos_first_date",
    "other_antibody_test_results": "covid_test_blood_result",
    "other_antibody_test_location": "covid_test_blood_where",
    "other_pcr_test_since_last_visit": "covid_test_swab",
    "other_pcr_test_last_negative_date": "covid_test_swab_neg_last_date",
    "other_pcr_test_first_positive_date": "covid_test_swab_pos_first_date",
    "other_pcr_test_results": "covid_test_swab_result",
    "is_regularly_lateral_flow_testing": "covid_testing_regular",
    "think_had_covid_admitted_to_hospital": "covid_admitted",
    "think_had_covid_date": "covid_date",
    "think_had_covid_contacted_nhs": "covid_nhs_contact",
    "think_had_covid": "covid_think_havehad",
    "days_since_think_had_covid": "covid_think_havehad_dayssince",
    "days_since_think_had_covid_group": "covid_think_havehad_dayssinceg",
    "think_had_covid_raw": "covid_think_havehad_orig",
    "ethnicity": "ethnicity",
    "cis_covid_vaccine_datetime": "covid_vaccine_date",
    "covid_vaccine_dose_1_datetime": "covid_vaccine_date1",
    "covid_vaccine_dose_2_datetime": "covid_vaccine_date2",
    "cis_covid_vaccine_received": "covid_vaccine_havehad",
    "cis_covid_vaccine_number_of_doses": "covid_vaccine_n_doses",
    "cis_covid_vaccine_offered": "covid_vaccine_offered",
    "cis_covid_vaccine_type": "covid_vaccine_type",
    "covid_vaccine_does_1_type": "covid_vaccine_type1",
    "covid_vaccine_does_2_type": "covid_vaccine_type2",
    "cis_covid_vaccine_type_other": "covid_vaccine_type_other",
    "ms2_pcr_cq_value": "ctMS2",
    "ms2_pcr_result_classification": "ctMS2_result",
    "n_gene_pcr_cq_value": "ctNgene",
    "n_gene_pcr_result_classification": "ctNgene_result",
    "orf1ab_gene_pcr_cq_value": "ctORF1ab",
    "orf1ab_gene_pcr_result_classification": "ctORF1ab_result",
    "s_gene_pcr_cq_value": "ctSgene",
    "s_gene_pcr_result_classification": "ctSgene_result",
    "mean_pcr_cq_value": "ct_mean",
    "one_positive_pcr_target_only": "ctonetargetonly",
    "cq_pattern": "ctpattern",
    "survey_response_dataset_major_version": "dataset",
    "did_not_attend_inferred": "did_not_attend",
    "other_survey_household_size": "dvhsize",
    "sex": "sex",
    "ethnicity_white": "ethnicity_wo",
    "ethnicity_group": "ethnicityg",
    "ever_care_home_worker": "ever_care_home_worker",
    "ever_had_long_term_health_condition": "ever_lthc",
    "ever_had_long_term_health_condition_or_disabled": "ever_lthc_disabled",
    "ever_work_person_facing_or_social_care": "ever_personfacing_socialcare",
    "ever_been_offered_vaccine": "ever_vaccine_offered",
    "household_fingerprick_status": "fingerprick_status_hh",
    "household_first_visit_datetime": "first_visit_date",
    "cis_flu_vaccine_received": "flu_vaccine_havehad",
    "linkage_via_pds": "found_pds",
    "soc_code_2010": "gold_code",
    "region_code": "gor9d",
    "region_code_surge": "gor9d_surge",
    "region_name": "gor_name",
    "region_name_surge": "gor_surge_name",
    "has_cis_covid_vaccination_data": "has_cis_vaccine",
    "work_health_care_regex_classification": "health_care_clean",
    "illness_lasting_over_12_months": "health_conditions",
    "illness_reduces_activity_or_ability": "health_conditions_impact",
    "household_enrolment_date": "hh_enrol_date",
    "pseudonymised_household_id": "hh_id_fake",
    "household_size": "hhsize",
    "hours_a_day_with_someone_else_at_home": "hours_someone_else_day",
    "index_multiple_deprivation": "imd_samp",
    "age_is_imputed": "imp_age",
    "ethnicity_is_imputed": "imp_eth",
    "sex_is_imputed": "imp_sex",
    "infant_1_age_months": "infant_1_age",
    "infant_2_age_months": "infant_2_age",
    "infant_3_age_months": "infant_3_age",
    "infant_4_age_months": "infant_4_age",
    "infant_5_age_months": "infant_5_age",
    "infant_6_age_months": "infant_6_age",
    "infant_7_age_months": "infant_7_age",
    "infant_8_age_months": "infant_8_age",
    "work_main_job_title": "job",
    "work_main_job_title_raw": "job_title",
    "pcr_lab_id": "lab_id",
    "last_attended_visit_datetime": "last_visit_date",
    "local_authority_unity_authority": "laua_name",
    "local_authority_unity_authority_code": "laua_samp",
    "long_covid_abdominal_pain": "long_covid_abdominal_pain",
    "long_covid_chest_pain": "long_covid_chest_pain",
    "long_covid_cough": "long_covid_cough",
    "long_covid_diarrhoea": "long_covid_diarrhoea",
    "long_covid_difficulty_concentrating": "long_covid_difficult_concentrate",
    "long_covid_fever": "long_covid_fever",
    "have_long_covid_symptoms": "long_covid_have_symptoms",
    "long_covid_headache": "long_covid_headache",
    "long_covid_loss_of_appetite": "long_covid_loss_of_appetite",
    "long_covid_loss_of_smell": "long_covid_loss_of_smell",
    "long_covid_loss_of_taste": "long_covid_loss_of_taste",
    "long_covid_low_mood": "long_covid_low_mood_not_enjoying",
    "long_covid_memory_loss_or_confusion": "long_covid_memory_loss_confusion",
    "long_covid_muscle_ache": "long_covid_muscle_ache",
    "long_covid_nausea_vomiting": "long_covid_nausea_vomiting",
    "long_covid_palpitations": "long_covid_palpitations",
    "long_covid_shortness_of_breath": "long_covid_shortness_of_breath",
    "long_covid_sore_throat": "long_covid_sore_throat",
    "long_covid_trouble_sleeping": "long_covid_trouble_sleeping",
    "long_covid_vertigo_dizziness": "long_covid_vertigo_dizziness",
    "long_covid_weakness_tiredness": "long_covid_weakness_tiredness",
    "long_covid_worry_anxiety": "long_covid_worry_anxiety",
    "lower_super_output_area_code_11": "lsoa11_samp",
    "work_main_job_changed": "main_job_changed",
    "sex_raw": "sex_raw",
    "outward_postcode": "outer_postcode",
    "household_participant_count": "n_participants",
    "people_in_household_count": "n_participants_corrected",
    "nims_linkage_status": "nims_category",
    "nims_vaccine_classification": "nims_class_vaccine",
    "nims_vaccine_dose_1_date_string": "nims_vaccine_date1",
    "nims_vaccine_dose_2_date_string": "nims_vaccine_date2",
    "nims_vaccine_dose_1_time": "nims_vaccine_time1",
    "nims_vaccine_dose_2_time": "nims_vaccine_time2",
    "nims_vaccine_dose_1_type": "nims_vaccine_type1",
    "nims_vaccine_dose_2_type": "nims_vaccine_type2",
    "no_fingerprick_blood_taken_reason": "no_blood_reason_fingerprick",
    "no_venous_blood_taken_reason": "no_blood_reason_venous",
    "not_consented_household_count": "not_consented_num",
    "not_consented_household_reason": "not_consented_reason",
    "not_present_household_count": "not_present_num",
    "not_present_household_reason": "not_present_reason",
    "person_1_not_consenting_age": "notconsent_1_age",
    "person_1_reason_for_not_consenting": "notconsent_1_reason",
    "person_2_not_consenting_age": "notconsent_2_age",
    "person_2_reason_for_not_consenting": "notconsent_2_reason",
    "person_3_not_consenting_age": "notconsent_3_age",
    "person_3_reason_for_not_consenting": "notconsent_3_reason",
    "person_4_not_consenting_age": "notconsent_4_age",
    "person_4_reason_for_not_consenting": "notconsent_4_reason",
    "person_5_not_consenting_age": "notconsent_5_age",
    "person_5_reason_for_not_consenting": "notconsent_5_reason",
    "person_6_not_consenting_age": "notconsent_6_age",
    "person_6_reason_for_not_consenting": "notconsent_6_reason",
    "person_7_not_consenting_age": "notconsent_7_age",
    "person_7_reason_for_not_consenting": "notconsent_7_reason",
    "person_8_not_consenting_age": "notconsent_8_age",
    "person_8_reason_for_not_consenting": "notconsent_8_reason",
    "person_9_not_consenting_age": "notconsent_9_age",
    "person_9_reason_for_not_consenting": "notconsent_9_reason",
    "person_not_present_1_age": "notpresent_1_age",
    "person_not_present_2_age": "notpresent_2_age",
    "person_not_present_3_age": "notpresent_3_age",
    "person_not_present_4_age": "notpresent_4_age",
    "person_not_present_5_age": "notpresent_5_age",
    "person_not_present_6_age": "notpresent_6_age",
    "person_not_present_7_age": "notpresent_7_age",
    "person_not_present_8_age": "notpresent_8_age",
    "prefer_receive_vouchers": "voucher_preference",
    "lfs_adults_in_household_count": "numadult",
    "lfs_children_in_household_count": "numchild",
    "ons_sample_origin": "ons_sample",
    "times_hour_or_longer_another_home_last_7_days": "others_home_hours",
    "times_hour_or_longer_another_person_your_home_last_7_days": "others_own_home_hours",
    "postcode": "postcode",
    "times_shopping_last_7_days": "outside_shopping_only_times",
    "times_outside_shopping_or_socialising_last_7_days": "outside_shopping_times",
    "times_socialise_last_7_days": "outside_socialising_only_times",
    "any_household_members_over_2_years_and_not_present": "over_2y_not_present_num",
    "accepted_fingerprick_invite": "accepted_fingerprick_invite",
    "work_patient_facing_now_regex_classification": "patient_facing_clean",
    "work_patient_facing_ever_regex_classification": "patient_facing_clean_ever",
    "conflicting_linkage_pds": "pds_conflict",
    "antibody_test_plate_common_id": "plate",
    "antibody_test_plate_id_s_protein": "plate_tdi",
    "antibody_test_plate_id_n_protein": "plate_tdi_n",
    "work_main_job_role": "main_resp",
    "reconsented_blood": "re_consent_blood",
    "work_main_job_role_raw": "main_resp_raw",
    "blood_sample_received_date_s_protein": "received_ox_date",
    "blood_sample_received_date_n_protein": "received_ox_date_n",
    "received_shielding_letter": "received_shielding_letter",
    "long_covid_reduce_activities": "reduce_activities_long_covid",
    "antibody_test_result_combined": "result_combined",
    "pcr_result_classification": "result_mk",
    "pcr_result_recorded_date_string": "result_mk_date",
    "pcr_result_recorded_datetime": "result_mk_date_time",
    "result_siemens_s_protein": "result_siemens",
    "result_siemens_n_protein": "result_siemens_n",
    "antibody_test_result_classification_s_protein": "result_tdi",
    "antibody_test_result_recorded_date_s_protein": "result_tdi_date",
    "antibody_test_result_recorded_date_n_protein": "result_tdi_date_n",
    "antibody_test_result_classification_n_protein": "result_tdi_n",
    "result_edited_to_negative": "result_to_negative",
    "result_edited_to_positive": "result_to_positive",
    "result_edited_to_void": "result_to_void",
    "rural_or_urban_classification": "rural_urban",
    "samples_taken_date_string": "samples_taken_date",
    "samples_taken_datetime_string": "samples_taken_date_time",
    "school_year_england_wales": "school_year",
    "school_year_northern_ireland": "school_year_NI",
    "school_year_scotland": "school_year_Scot",
    "self_isolating": "self_isolating",
    "self_isolating_detailed": "self_isolating_v1",
    "participant_id": "participant_id",
    "withdrawal_reason": "reason_for_withdrawal",
    "withdrawal_reason_raw": "withdrawal_reason",
    "have_ever_smoked_regularly": "smoke_ever_regularly",
    "smokes_cigar": "smoke_now_cigar",
    "smoke_cigarettes": "smoke_now_cigarettes",
    "smokes_hookah_shisha_pipes": "smoke_now_hookah_shisha",
    "smokes_nothing_now": "smoke_now_nothing",
    "smokes_pipe": "smoke_now_pipe",
    "smokes_vape_e_cigarettes": "smoke_now_vape",
    "surge_testing_flag": "surge_flag",
    "swab_sample_barcode": "swab_barcode_cleaned",
    "swab_taken": "swab_taken",
    "symptoms_around_cghfevamn_symptom_group": "sympt_around_cghfevamn",
    "symptoms_since_last_visit_abdominal_pain": "sympt_covid_abdominal_pain",
    "think_have_covid_any_symptoms": "sympt_covid_any",
    "think_have_covid_any_symptoms_raw": "sympt_covid_any_orig",
    "think_have_covid_cghfevamn_symptom_group": "sympt_covid_cghfevamn",
    "symptoms_since_last_visit_cough": "sympt_covid_cough",
    "symptoms_since_last_visit_count": "sympt_covid_count",
    "symptoms_since_last_visit_diarrhoea": "sympt_covid_diarrhoea",
    "symptoms_since_last_visit_fatigue_weakness": "sympt_covid_fatigue_weakness",
    "symptoms_since_last_visit_fever": "sympt_covid_fever",
    "symptoms_since_last_visit_headache": "sympt_covid_headache",
    "symptoms_since_last_visit_loss_of_appetite": "sympt_covid_loss_of_appetite",
    "symptoms_since_last_visit_loss_of_smell": "sympt_covid_loss_of_smell",
    "symptoms_since_last_visit_loss_of_taste": "sympt_covid_loss_of_taste",
    "symptoms_since_last_visit_muscle_ache_myalgia": "sympt_covid_muscle_ache_myalgia",
    "symptoms_since_last_visit_nausea_vomiting": "sympt_covid_nausea_vomiting",
    "symptoms_since_last_visit_noisy_breathing": "sympt_covid_noisy_breathing",
    "symptoms_since_last_visit_runny_nose_sneezing": "sympt_covid_runny_nose_sneezing",
    "symptoms_since_last_visit_shortness_of_breath": "sympt_covid_shortness_of_breath",
    "symptoms_since_last_visit_sore_throat": "sympt_covid_sore_throat",
    "symptoms_since_last_visit_more_trouble_sleeping": "sympt_covid_more_trouble_sleeping",
    "symptoms_last_7_days_abdominal_pain": "sympt_now_abdominal_pain",
    "symptoms_last_7_days_any": "sympt_now_any12",
    "symptoms_last_7_days_any_raw": "sympt_now_any_orig",
    "symptoms_last_7_days_cghfevamn_symptom_group": "sympt_now_cghfevamn",
    "symptoms_last_7_days_cough": "sympt_now_cough",
    "symptoms_last_7_days_symptom_count": "sympt_now_count12",
    "symptoms_last_7_days_onset_date": "sympt_now_date",
    "symptoms_last_7_days_diarrhoea": "sympt_now_diarrhoea",
    "symptoms_last_7_days_fatigue_weakness": "sympt_now_fatigue_weakness",
    "symptoms_last_7_days_fever": "sympt_now_fever",
    "symptoms_last_7_days_headache": "sympt_now_headache",
    "symptoms_last_7_days_loss_of_appetite": "sympt_now_loss_of_appetite_or_e",
    "symptoms_last_7_days_loss_of_smell": "sympt_now_loss_of_smell",
    "symptoms_last_7_days_loss_of_taste": "sympt_now_loss_of_taste",
    "symptoms_last_7_days_more_trouble_sleeping": "sympt_now_more_trouble_sleeping",
    "symptoms_last_7_days_muscle_ache_myalgia": "sympt_now_muscle_ache_myalgia",
    "symptoms_last_7_days_nausea_vomiting": "sympt_now_nausea_vomiting",
    "symptoms_last_7_days_noisy_breathing_wheezing": "sympt_now_noisy_breathing_wheez",
    "symptoms_last_7_days_runny_nose_sneezing": "sympt_now_runny_nose_sneezing",
    "symptoms_last_7_days_shortness_of_breath": "sympt_now_shortness_of_breath",
    "symptoms_last_7_days_sore_throat": "sympt_now_sore_throat",
    "tenure_group": "tenure_group",
    "think_have_covid_symptoms": "think_have_covid_sympt_now",
    "been_outside_uk_since_april_2020": "travel_abroad",
    "been_outside_uk_last_country": "travel_abroad_country",
    "been_outside_uk_last_date": "travel_abroad_date",
    "cis_vaccine_type_imputed": "type_imputation_flag_CIS",
    "any_household_members_under_2_years": "under_2y_num",
    "visit_date_string": "visit_date",
    "participant_survey_status": "withdrawn_participant",
    "days_since_enrolment": "visit_day",
    "participant_testing_group": "work_type",
    "visit_id": "visit_id",
    "visit_number": "visit_num",
    "visit_order": "visit_order",
    "visit_datetime": "visit_date_time",
    "blood_sample_collected_datetime": "voyager_blood_dt_time",
    "antibody_test_well_id": "well_tdi",
    "response_id": "visit_id",
    "participant_visit_status": "visit_status",
    "work_nursing_or_residential_care_home": "work_care_nursing_home",
    "work_direct_contact_patients_clients": "work_direct_contact_patients_etc",
    "work_health_care_combined": "work_healthcare",
    "work_health_care_v0_raw": "work_healthcare_orig",
    "work_health_care_v1_v2_raw": "work_healthcare_v1",
    "work_location": "work_location",
    "work_location_raw": "work_location_orig",
    "work_not_from_home_days_per_week": "work_outside_home_days",
    "work_not_from_home_days_per_week_raw": "work_outside_home_days_orig",
    "work_patient_facing_now": "work_patientfacing_now",
    "work_person_facing_now": "work_personfacing_now",
    "work_sectors": "work_sector",
    "work_sectors_other": "work_sector_other_text",
    "ability_to_socially_distance_at_work_or_school": "work_social_distancing",
    "work_social_care": "work_socialcare",
    "work_social_care_raw": "work_socialcare_orig",
    "work_status_combined": "work_status",
    "work_status_regex_classification": "work_status_clean",
    "work_status_combined_raw": "work_status_orig",
    "work_status_v1": "work_status_v1",
    "work_status_v1_raw": "work_status_v1_orig",
    "work_status_v2": "work_status_v2",
    "work_status_v2_raw": "work_status_v2_orig",
    "transport_to_work_or_school": "work_travel",
    "visit_type": "visit_type",
    "n_gene_pcr_target": "ch2target",
    "s_gene_pcr_target": "ch3target",
    "household_weeks_since_survey_enrolment": "hh_enrol_week",
    "confirm_received_vouchers": "confirm_received_vouchers",
    "plate_storage_method": "platestorage",
    "withdrawal_type": "withdrawal_type",
    "not_attended_reason": "not_attended_reason",
    "improved_visit_date": "improved_visit_date",
    "deferred": "deferred",
}
