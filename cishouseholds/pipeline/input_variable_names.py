swab_variable_name_map = {
    "Sample": "swab_sample_barcode",
    "Result": "pcr_result_classification",
    "Date Tested": "pcr_datetime",
    "Lab ID": "pcr_lab_id",
    "testKit": "pcr_method",
    "CH1-Target": "orf1ab_gene_pcr_target",
    "CH1-Result": "orf1ab_gene_pcr_result_classification",
    "CH1-Cq": "orf1ab_gene_pcr_cq_value",
    "CH2-Target": "n_gene_pcr_target",
    "CH2-Result": "n_gene_pcr_result_classification",
    "CH2-Cq": "n_gene_pcr_cq_value",
    "CH3-Target": "s_gene_pcr_target",
    "CH3-Result": "s_gene_pcr_result_classification",
    "CH3-Cq": "s_gene_pcr_cq_value",
    "CH4-Target": "ms2_pcr_target",
    "CH4-Result": "ms2_pcr_result_classification",
    "CH4-Cq": "ms2_pcr_cq_value",
}


blood_variable_name_map = {
    "Serum Source ID": "blood_sample_barcode",
    "Blood Sample Type": "blood_sample_type",
    "Plate Barcode": "antibody_test_plate_id",
    "Well ID": "antibody_test_well_id",
    "Detection": "antibody_test_result_classification",
    "Monoclonal quantitation (Colourimetric)": "antibody_test_result_value",
    "Monoclonal bounded quantitation (Colourimetric)": "antibody_test_bounded_result_value",
    "Monoclonal undiluted quantitation (Colourimetric)": "antibody_test_undiluted_result_value",
    "Date ELISA Result record created": "antibody_test_result_recorded_date",
    "Date Samples Arrayed Oxford": "blood_sample_arrayed_date",
    "Date Samples Received Oxford": "blood_sample_received_date",
    "Voyager Date Created": "blood_sample_collected_datetime",
}

sample_eng_wl_sc_variable_name_map = {
    "UAC": "unique_access_code",
    "LA_CODE": "local_authority_code",
    "Bloods": "in_blood_cohort",
    "oa11": "output_area_code",
    "laua": "local_authority_unity_authority_code",
    "ctry": "country_code",
    "CUSTODIAN_REGION_CODE": "custodian_region_code",
    "lsoa11": "lower_super_output_area_code",
    "msoa11": "middle_super_output_area_code",
    "ru11ind": "rural_urban_classification",
    "oac11": "census_output_area_classification",
    "rgn": "region_code",
    "imd": "index_multiple_deprivation",
    "interim_id": "cis_area_indicator",
}

sample_ni_variable_name_map = {
    "UAC": "unique_access_code",
    "sample": "sample_week_indicator",
    "oa11": "output_area_code",
    "laua": "local_authority_unity_authority_code",
    "ctry": "country_code",
    "GOR9D": "region_code",
    "lsoa11": "lower_super_output_area_code",
    "msoa11": "middle_super_output_area_code",
    "oac11": "census_output_area_classification",
    "LSOA11NM": "lower_super_output_area_name",
    "CIS20CD": "cis_area_code",
    "rgn": "region_code",
    "imd": "index_multiple_deprivation",
    "interim_id": "cis_area_indicator",
}

unprocessed_bloods_variable_name_map = {
    "Date Recieved": "date_received",
    "Sample ID": "sample_id",
    "Rejection code": "rejection_code",
    "Reason for rejection": "rejection_reason",
    "Sample Type V/C": "sample_type",
}

survey_responses_v2_variable_name_map = {
    "ons_household_id": "ons_household_id",
    "Visit_ID": "visit_id",
    "Visit Status": "visit_status",
    "Participant_Visit_status": "participant_visit_status",
    "Participant_status": "participant_status",
    "Withdrawal_reason": "withdrawal_reason",
    "Type_of_Visit": "type_of_visit",
    "Visit_Order": "visit_order",
    "Work_Type_Picklist": "participant_testing_group",
    "Visit_Date_Time": "visit_datetime",
    "Street": "street",
    "City": "city",
    "County": "county",
    "Postcode": "postcode",
    "Cohort": "study_cohort",
    "Fingerprick_Status": "fingerprick_status_household",
    "Household_Members_Under_2_Years": "household_members_under_2_years",
    "Infant_1": "infant_1_age",
    "Infant_2": "infant_2_age",
    "Infant_3": "infant_3_age",
    "Infant_4": "infant_4_age",
    "Infant_5": "infant_5_age",
    "Infant_6": "infant_6_age",
    "Infant_7": "infant_7_age",
    "Infant_8": "infant_8_age",
    "Household_Members_Over_2_and_Not_Present": "household_members_over_2_years_and_not_present",
    "Person_1": "person_1_age",
    "Person_2": "person_2_age",
    "Person_3": "person_3_age",
    "Person_4": "person_4_age",
    "Person_5": "person_5_age",
    "Person_6": "person_6_age",
    "Person_7": "person_7_age",
    "Person_8": "person_8_age",
    "Person_1_Not_Consenting_Age": "person_1_not_consenting_age",
    "Person1_Reason_for_Not_Consenting": "person_1_reason_for_not_consenting",
    "Person_2_Not_Consenting_Age": "person_2_not_consenting_age",
    "Person2_Reason_for_Not_Consenting": "person_2_reason_for_not_consenting",
    "Person_3_Not_Consenting_Age": "person_3_not_consenting_age",
    "Person3_Reason_for_Not_Consenting": "person_3_reason_for_not_consenting",
    "Person_4_Not_Consenting_Age": "person_4_not_consenting_age",
    "Person4_Reason_for_Not_Consenting": "person_4_reason_for_not_consenting",
    "Person_5_Not_Consenting_Age": "person_5_not_consenting_age",
    "Person5_Reason_for_Not_Consenting": "person_5_reason_for_not_consenting",
    "Person_6_Not_Consenting_Age": "person_6_not_consenting_age",
    "Person6_Reason_for_Not_Consenting": "person_6_reason_for_not_consenting",
    "Person_7_Not_Consenting_Age": "person_7_not_consenting_age",
    "Person7_Reason_for_Not_Consenting": "person_7_reason_for_not_consenting",
    "Person_8_Not_Consenting_Age": "person_8_not_consenting_age",
    "Person8_Reason_for_Not_Consenting": "person_8_reason_for_not_consenting",
    "Person_9_Not_Consenting_Age": "person_9_not_consenting_age",
    "Person9_Reason_for_Not_Consenting": "person_9_reason_for_not_consenting",
    "Participant_id": "participant_id",
    "Title": "title",
    "First_Name": "first_name",
    "Middle_Name": "middle_name",
    "Last_Name": "last_name",
    "DoB": "date_of_birth",
    "Email": "email",
    "Have_landline_number": "have_landline_number",
    "Have_mobile_number": "have_mobile_number",
    "Have_email_address": "have_email_address",
    "Prefer_receive_vouchers": "prefer_receive_vouchers",
    "Confirm_receive_vouchers": "confirm_receive_vouchers",
    "No_Email_address": "no_email_address",
    "Able_to_take_blood": "able_to_take_blood",
    "No_Blood_reason_fingerprick": "no_blood_reason_fingerprick",
    "No_Blood_reason_venous": "no_blood_reason_venous",
    "bloods_barcode_1": "blood_sample_barcode",
    "Swab_Barcode_1": "swab_sample_barcode",
    "Date_Time_Samples_Taken": "samples_taken_datetime",
    "Sex": "sex",
    "Gender": "gender",
    "Ethnic_group": "ethnic_group",
    "Ethnicity": "ethnicity",
    "Ethnicity_Other": "ethnicity_other",
    "Consent_to_First_Visit": "consent_1_visit",
    "Consent_to_Five_Visits": "consent_5_visits",
    "Consent_to_April_22": "consent_april_22",
    "Consent_to_Sixteen_Visits": "consent_16_visits",
    "Consent_to_Blood_Test": "consent_blood_test",
    "Consent_to_Finger_prick_A1_A3": "consent_finger_prick_a1_a3",
    "Consent_to_extend_study_under_16_B1_B3": "consent_extend_study_under_16_b1_b3",
    "Consent_to_be_Contacted_Extra_Research": "consent_contact_extra_research",
    "Consent_to_be_Contacted_Extra_ResearchYN": "consent_contact_extra_researchyn",
    "Consent_to_use_of_Surplus_Blood_Samples": "consent_use_of_surplus_blood_samples",
    "Consent_to_use_of_Surplus_Blood_SamplesYN": "consent_use_of_surplus_blood_samplesyn",
    "Approached_for_blood_samples?": "approached_for_blood_samples",
    "Consent_to_blood_samples_if_positive": "consent_blood_samples_if_positive",
    "Consent_to_blood_samples_if_positiveYN": "consent_blood_samples_if_positiveyn",
    "Consent_to_fingerprick_blood_samples": "consent_fingerprick_blood_samples",
    "Accepted_invite_to_fingerprick": "accepted_invite_fingerprick",
    "Re_consented_for_blood": "reconsented_blood",
    "What_is_the_title_of_your_main_job": "work_main_job_title",
    "What_do_you_do_in_your_main_job_business": "work_main_job_role",
    "Occupations_sectors_do_you_work_in": "work_sectors",
    "occupation_sector_other": "work_sectors_other",
    "Work_in_a_nursing_residential_care_home": "work_nursing_or_residential_care_home",
    "Do_you_currently_work_in_healthcare": "work_health_care",
    "Direct_contact_patients_clients_resid": "work_direct_contact_persons",
    "Have_physical_mental_health_or_illnesses": "illness_lasting_over_12_months",
    "physical_mental_health_or_illness_reduces_activity_ability": "illness_reduces_activity_or_ability",
    "Have_you_ever_smoked_regularly": "have_ever_smoked_regularly",
    "Do_you_currently_smoke_or_vape": "smokes_or_vapes_description",
    "Do_you_currently_smoke_or_vape_at_all": "smokes_or_vapes",
    "Smoke_Yes_cigarettes": "smoke_cigarettes",
    "Smoke_Yes_cigar": "smokes_cigar",
    "Smoke_Yes_pipe": "smokes_pipe",
    "Smoke_Yes_vape_e_cigarettes": "smokes_vape_e_cigarettes",
    "Smoke_Hookah/shisha pipes": "smokes_hookah_shisha_pipes",
    "What_is_your_current_working_status": "work_status",
    "Paid_employment": "work_in_additional_paid_employment",
    "Main_Job_Changed": "work_main_job_changed",
    "Where_are_you_mainly_working_now": "work_location",
    "How_often_do_you_work_elsewhere": "work_not_from_home_days_per_week",
    "How_do_you_get_to_and_from_work_school": "transport_to_work_or_school",
    "Can_you_socially_distance_at_work": "ability_to_socially_distance_at_work_or_school",
    "Had_symptoms_in_the_last_7_days": "had_symptoms_last_7_days",
    "Which_symptoms_in_the_last_7_days": "which_symptoms_last_7_days",
    "Date_of_first_symptom_onset": "symptoms_last_7_days_onset_date",
    "Symptoms_7_Fever": "symptoms_last_7_days_fever",
    "Symptoms_7_Muscle_ache_myalgia": "symptoms_last_7_days_muscle_ache_myalgia",
    "Symptoms_7_Fatigue_weakness": "symptoms_last_7_days_fatigue_weakness",
    "Symptoms_7_Sore_throat": "symptoms_last_7_days_sore_throat",
    "Symptoms_7_Cough": "symptoms_last_7_days_cough",
    "Symptoms_7_Shortness_of_breath": "symptoms_last_7_days_shortness_of_breath",
    "Symptoms_7_Headache": "symptoms_last_7_days_headache",
    "Symptoms_7_Nausea_vomiting": "symptoms_last_7_days_nausea_vomiting",
    "Symptoms_7_Abdominal_pain": "symptoms_last_7_days_abdominal_pain",
    "Symptoms_7_Diarrhoea": "symptoms_last_7_days_diarrhoea",
    "Symptoms_7_Loss_of_taste": "symptoms_last_7_days_loss_of_taste",
    "Symptoms_7_Loss_of_smell": "symptoms_last_7_days_loss_of_smell",
    "Are_you_self_Isolating_S2": "is_self_isolating",
    "Do_you_think_you_have_Covid_Symptoms": "think_have_covid_symptoms",
    "Contact_Known_Positive_COVID19_28_days": "contact_known_positive_covid_last_28_days",
    "If_Known_last_contact_date": "last_covid_contact_date",
    "If_Known_type_of_contact_S2": "last_covid_contact_location",
    "Contact_Suspect_Positive_COVID19_28_d": "contact_suspect_positive_covid_last_28_days",
    "If_suspect_last_contact_date": "last_suspected_covid_contact_date",
    "If_suspect_type_of_contact_S2": "last_suspected_covid_contact_location",
    "You_been_Hospital_last_28_days": "hospital_last_28_days",
    "OtherHouse_been_Hospital_last_28_days": "hospital_last_28_days_other_household_member",
    "Your_been_in_Care_Home_last_28_days": "care_home_last_28_days",
    "OtherHouse_been_in_Care_Home_last_28_days": "care_home_last_28_days_other_household_member",
    "Hours_a_day_with_someone_else": "hours_a_day_with_someone_else_at_home",
    "Physical_Contact_18yrs": "physical_contact_under_18_years",
    "Physical_Contact_18_to_69_yrs": "physical_contact_18_to_69_years",
    "Physical_Contact_70_yrs": "physical_contact_over_70_years",
    "Social_Distance_Contact_18yrs": "social_distance_contact_under_18_years",
    "Social_Distance_Contact_18_to_69_yrs": "social_distance_contact_18_to_69_years",
    "Social_Distance_Contact_70_yrs": "social_distance_contact_over_70_years",
    "1Hour_or_Longer_another_person_home": "times_hour_or_longer_another_home_last_7_days",
    "1Hour_or_Longer_another_person_yourhome": "times_hour_or_longer_another_person_your_home_last_7_days",
    "Times_Outside_Home_For_Shopping": "times_outside_shopping_or_socialising_last_7_days",
    "Shopping_last_7_days": "times_shopping_last_7_days",
    "Socialise_last_7_days": "times_socialise_last_7_days",
    "Regular_testing_COVID": "is_regularly_lateral_flow_testing",
    "Face_Covering_or_Mask_outside_of_home": "face_covering_outside_of_home",
    "Face_Mask_Work_Place": "face_covering_work",
    "Face_Mask_Other_Enclosed_Places": "face_covering_other_enclosed_places",
    "Do_you_think_you_have_had_Covid_19": "think_had_covid",
    "think_had_covid_19_any_symptoms": "think_had_covid_any_symptoms",
    "think_had_covid_19_which_symptoms": "think_had_covid_which_symptoms",
    "Previous_Symptoms_Fever": "symptoms_since_last_visit_fever",
    "Previous_Symptoms_Muscle_ache_myalgia": "symptoms_since_last_visit_muscle_ache_myalgia",
    "Previous_Symptoms_Fatigue_weakness": "symptoms_since_last_visit_fatigue_weakness",
    "Previous_Symptoms_Sore_throat": "symptoms_since_last_visit_sore_throat",
    "Previous_Symptoms_Cough": "symptoms_since_last_visit_cough",
    "Previous_Symptoms_Shortness_of_breath": "symptoms_since_last_visit_shortness_of_breath",
    "Previous_Symptoms_Headache": "symptoms_since_last_visit_headache",
    "Previous_Symptoms_Nausea_vomiting": "symptoms_since_last_visit_nausea_vomiting",
    "Previous_Symptoms_Abdominal_pain": "symptoms_since_last_visit_abdominal_pain",
    "Previous_Symptoms_Diarrhoea": "symptoms_since_last_visit_diarrhoea",
    "Previous_Symptoms_Loss_of_taste": "symptoms_since_last_visit_loss_of_taste",
    "Previous_Symptoms_Loss_of_smell": "symptoms_since_last_visit_loss_of_smell",
    "If_yes_Date_of_first_symptoms": "think_had_covid_date",
    "Did_you_contact_NHS": "think_had_covid_contacted_nhs",
    "Were_you_admitted_to_hospital": "think_had_covid_admitted_to_hospital",
    "Have_you_had_a_swab_test": "other_pcr_test_since_last_visit",
    "If_Yes_What_was_result": "other_pcr_test_results",
    "If_positive_Date_of_1st_ve_test": "other_pcr_test_first_positive_date",
    "If_all_negative_Date_last_test": "other_pcr_test_last_negative_date",
    "Have_you_had_a_blood_test_for_Covid": "other_antibody_test_since_last_visit",
    "What_was_the_result_of_the_blood_test": "other_antibody_test_results",
    "Where_was_the_test_done": "other_antibody_test_location",
    "If_ve_Blood_Date_of_1st_ve_test": "other_antibody_test_first_positive_date",
    "If_all_ve_blood_Date_last_ve_test": "other_antibody_test_last_negative_date",
    "Have_Long_Covid_Symptoms": "have_long_covid_symptoms",
    "Long_Covid_Reduce_Activities": "long_covid_reduce_activities",
    "Long_Covid_Symptoms": "long_covid_symptoms",
    "Long_Covid_Fever": "long_covid_fever",
    "Long_Covid_Weakness_tiredness": "long_covid_weakness_tiredness",
    "Long_Covid_Diarrhoea": "long_covid_diarrhoea",
    "Long_Covid_Loss_of_smell": "long_covid_loss_of_smell",
    "Long_Covid_Shortness_of_breath": "long_covid_shortness_of_breath",
    "Long_Covid_Vertigo_dizziness": "long_covid_vertigo_dizziness",
    "Long_Covid_Trouble_sleeping": "long_covid_trouble_sleeping",
    "Long_Covid_Headache": "long_covid_headache",
    "Long_Covid_Nausea_vomiting": "long_covid_nausea_vomiting",
    "Long_Covid_Loss_of_appetite": "long_covid_loss_of_appetite",
    "Long_Covid_Sore_throat": "long_covid_sore_throat",
    "Long_Covid_Chest_pain": "long_covid_chest_pain",
    "Long_Covid_Worry_anxiety": "long_covid_worry_anxiety",
    "Long_Covid_Memory_loss_or_confusion": "long_covid_memory_loss_or_confusion",
    "Long_Covid_Muscle_ache": "long_covid_muscle_ache",
    "Long_Covid_Abdominal_pain": "long_covid_abdominal_pain",
    "Long_Covid_Loss_of_taste": "long_covid_loss_of_taste",
    "Long_Covid_Cough": "long_covid_cough",
    "Long_Covid_Palpitations": "long_covid_palpitations",
    "Long_Covid_Low_mood_not_enjoying_anything": "long_covid_low_mood",
    "Long_Covid_Difficulty_concentrating": "long_covid_difficulty_concentrating",
    "Have_you_been_offered_a_vaccination": "cis_covid_vaccine_offered",
    "Vaccinated_Against_Covid": "cis_covid_vaccine_received",
    "Type_Of_Vaccination": "cis_covid_vaccine_type",
    "Vaccination_Other": "cis_covid_vaccine_type_other",
    "Number_Of_Doses": "cis_covid_vaccine_number_of_doses",
    "Date_Of_Vaccination": "cis_covid_vaccine_date",
    "Have_you_been_outside_UK_since_April": "outside_uk_since_april_2020",
    "been_outside_uk_last_country": "outside_uk_last_country",
    "been_outside_uk_last_date": "outside_uk_last_date",
    "Have_you_been_outside_UK_Lastspoke": "outside_uk_since_last_visit",
}
