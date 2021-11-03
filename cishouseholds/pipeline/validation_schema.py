swab_allowed_pcr_results = ["Inconclusive", "Negative", "Positive", "Rejected"]

swab_validation_schema = {
    "Sample": {"type": "string", "regex": r"ONS\d{8}"},
    "Result": {"type": "string", "allowed": ["Negative", "Positive", "Void"]},
    "Date Tested": {"type": "string", "nullable": True},
    "Lab ID": {"type": "string"},
    "testKit": {"type": "string"},
    "CH1-Target": {"type": "string", "allowed": ["ORF1ab"]},
    "CH1-Result": {"type": "string", "allowed": swab_allowed_pcr_results},
    "CH1-Cq": {"type": "double", "nullable": True, "min": 0},
    "CH2-Target": {"type": "string", "allowed": ["N gene"]},
    "CH2-Result": {"type": "string", "allowed": swab_allowed_pcr_results},
    "CH2-Cq": {"type": "double", "nullable": True, "min": 0},
    "CH3-Target": {"type": "string", "allowed": ["S gene"]},
    "CH3-Result": {"type": "string", "allowed": swab_allowed_pcr_results},
    "CH3-Cq": {"type": "double", "nullable": True, "min": 0},
    "CH4-Target": {"type": "string", "allowed": ["MS2"]},
    "CH4-Result": {"type": "string", "allowed": swab_allowed_pcr_results},
    "CH4-Cq": {"type": "double", "nullable": True, "min": 0},
}


blood_validation_schema = {
    "Serum Source ID": {"type": "string", "regex": r"ONS\d{8}"},
    "Blood Sample Type": {"type": "string", "allowed": ["Venous", "Capillary"]},
    "Plate Barcode": {"type": "string", "regex": r"(ON[BS]|MIX)_[0-9]{6}[C|V]S(-[0-9]+)"},
    "Well ID": {"type": "string", "regex": r"[A-Z][0-9]{2}"},
    "Detection": {"type": "string", "allowed": ["DETECTED", "NOT detected", "failed"]},
    "Monoclonal quantitation (Colourimetric)": {"type": "double", "nullable": True, "min": 0},
    "Monoclonal bounded quantitation (Colourimetric)": {"type": "string"},
    "Monoclonal undiluted quantitation (Colourimetric)": {"type": "string"},
    "Date ELISA Result record created": {"type": "string", "nullable": True},
    "Date Samples Arrayed Oxford": {"type": "string", "nullable": True},
    "Date Samples Received Oxford": {"type": "string", "nullable": True},
    "Voyager Date Created": {"type": "string", "nullable": True},
}

historic_blood_validation_schema = {
    "ons_id": {"type": "string"},
    "Blood Sample Type": {"type": "string"},
    "Plate Barcode": {"type": "string"},
    "Well ID": {"type": "string"},
    "Detection": {"type": "string"},
    "Monoclonal quantitation (Colourimetric)": {"type": "float"},
    "Date ELISA Result record created": {"type": "string"},
    "Date Samples Arrayed Oxford": {"type": "string"},
    "Date Samples Received Oxford": {"type": "string"},
    "Voyager Date Created": {"type": "string"},
    "siemens_interpretation": {"type": "string"},
    "tdi_assay_net_signal": {"type": "integer"},
    "siemens_reading": {"type": "string"},
    "lims_id": {"type": "string"},
}

sample_direct_eng_wc_schema = {
    "unique_access_code": {"type": "string", "regex": r"^\d{12}"},
    "local_authority_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
    "in_blood_cohort": {"type": "integer", "min": 0, "max": 1},
    "output_area_code": {"type": "string", "regex": r"^[E,W,S]00\d{6}"},
    "local_authority_unity_authority_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
    "country_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
    "custodian_region_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
    "lower_super_output_area_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
    "middle_super_output_area_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
    "rural_urban_classification": {"type": "string", "regex": r"^([a-zA-Z]\d{1}|\d{1})"},
    "census_output_area_classification": {"type": "string", "regex": r"^\d{1}[a-zA-Z]\d{1}"},
    "region_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
    "index_multiple_deprivation": {"type": "integer", "min": 0, "max": 32844},
    "cis_area_indicator": {"type": "integer", "min": 1, "max": 128},
}

sample_northern_ireland_schema = {
    "unique_access_code": {"type": "string", "regex": r"^\d{12}"},
    "sample_week_indicator": {"type": "string", "regex": r"^\d{1}[a-zA-Z]{3}"},
    "output_area_code": {"type": "string", "regex": r"^N00\d{6}"},
    "local_authority_unity_authority_code": {"type": "string", "regex": r"^N\d{8}"},
    "country_code": {"type": "string", "regex": r"^N\d{8}"},
    "custodian_region_code": {"type": "string", "regex": r"^N\d{8}"},
    "lower_super_output_area_code": {"type": "string", "regex": r"^N\d{8}"},
    "middle_super_output_area_code": {"type": "string", "regex": r"^N\d{8}"},
    "census_output_area_classification": {"type": "string", "regex": r"^\d{1}[a-zA-Z]\d{1}"},
    "lower_super_output_area_name": {"type": "string", "regex": r"[a-zA-Z]{2,}}"},
    "cis_area_code": {"type": "string", "regex": r"^J\d{8}"},
    "region_code": {"type": "string", "regex": r"^N\d{8}"},
    "index_multiple_deprivation": {"type": "integer", "min": 1, "max": 890},
    "cis_area_indicator": {"type": "integer", "min": 999, "max": 999},
}
survey_responses_v0_validation_schema = {
    "ONS Household ID": {"type": "string"},
    "Visit ID": {"type": "string"},
    "Type of Visit": {"type": "string"},
    "Participant_Visit_status": {"type": "string"},
    "Withdrawal_reason": {"type": "string"},
    "Visit Date/Time": {"type": "string"},
    "Street": {"type": "string"},
    "City": {"type": "string"},
    "County": {"type": "string"},
    "Postcode": {"type": "string"},
    "Phase": {"type": "string"},
    "No. Paticicpants not Consented": {"type": "integer"},
    "Reason Participants not Consented": {"type": "string"},
    "No. Participants not present for Visit": {"type": "integer"},
    "Reason Participants not present on Visit": {"type": "string"},
    "Any Household Members Under 2 Years": {"type": "string"},
    "Infant 1 age in Months": {"type": "integer"},
    "Infant 2 age in Months": {"type": "integer"},
    "Infant 3 age in Months": {"type": "integer"},
    "Infant 4 age in Months": {"type": "integer"},
    "Infant 5 age in Months": {"type": "integer"},
    "Infant 6 age in Months": {"type": "integer"},
    "Infant 7 age in Months": {"type": "integer"},
    "Infant 8 age in Months": {"type": "integer"},
    "Any Household Members Over 2 Not Present": {"type": "string"},
    "Person 1 age in Years": {"type": "integer"},
    "Person 2 age in Years": {"type": "integer"},
    "Person 3 age in Years": {"type": "integer"},
    "Person 4 age in Years": {"type": "integer"},
    "Person 5 age in Years": {"type": "integer"},
    "Person 6 age in Years": {"type": "integer"},
    "Person 7 age in Years": {"type": "integer"},
    "Person 8 age in Years": {"type": "integer"},
    "Person 1 Not Consenting (Age in Years)": {"type": "integer"},
    "Reason for Not Consenting (Person 1)": {"type": "string"},
    "Person 2 Not Consenting (Age in Years)": {"type": "integer"},
    "Reason for Not Consenting (Person 2)": {"type": "string"},
    "Person 3 Not Consenting (Age in Years)": {"type": "integer"},
    "Reason for Not Consenting (Person 3)": {"type": "string"},
    "Person 4 Not Consenting (Age in Years)": {"type": "integer"},
    "Reason for Not Consenting (Person 4)": {"type": "string"},
    "Person 5 Not Consenting (Age in Years)": {"type": "integer"},
    "Reason for Not Consenting (Person 5)": {"type": "string"},
    "Person 6 Not Consenting (Age in Years)": {"type": "integer"},
    "Reason for Not Consenting (Person 6)": {"type": "string"},
    "Person 7 Not Consenting (Age in Years)": {"type": "integer"},
    "Reason for Not Consenting (Person 7)": {"type": "string"},
    "Person 8 Not Consenting (Age in Years)": {"type": "integer"},
    "Reason for Not Consenting (Person 8)": {"type": "string"},
    "Person 9 Not Consenting (Age in Years)": {"type": "integer"},
    "Reason for Not Consenting (Person 9)": {"type": "string"},
    "Participant ID": {"type": "string"},
    "Suffix": {"type": "string"},
    "Full Name": {"type": "string"},
    "DoB": {"type": "string"},
    "Email": {"type": "string"},
    "No Email Address": {"type": "integer"},
    "Bloods Taken": {"type": "integer"},
    "Bloods Barcode 1": {"type": "string"},
    "Swab Taken": {"type": "integer"},
    "Swab Barcode 1": {"type": "string"},
    "Date/Time Samples Taken": {"type": "string"},
    "Sex": {"type": "string"},
    "Gender": {"type": "string"},
    "Ethnicity": {"type": "string"},
    "Consent to First Visit": {"type": "integer"},
    "Consent to Five Visits": {"type": "integer"},
    "Consent to Sixteen Visits": {"type": "integer"},
    "Consent to be Contacted Extra Research": {"type": "integer"},
    "Consent to Blood Samples": {"type": "integer"},
    "Consent to Surplus Blood Sample": {"type": "integer"},
    "Working Status (Main Job)": {"type": "string"},
    "Job Title": {"type": "string"},
    "Main Job Responsibilities": {"type": "string"},
    "Working Location": {"type": "string"},
    "No. days a week working outside of home?": {"type": "string"},
    "Do you work in healthcare?": {"type": "string"},
    "Do you work in social care?": {"type": "string"},
    "Do you have any of these symptoms Today?": {"type": "string"},
    "Symptoms today": {"type": "string"},
    "Symptoms today- Fever": {"type": "string"},
    "Symptoms today- Muscle ache (myalgia)": {"type": "string"},
    "Symptoms today- Fatigue (weakness)": {"type": "string"},
    "Symptoms today- Sore throat": {"type": "string"},
    "Symptoms today- Cough": {"type": "string"},
    "Symptoms today- Shortness of breath": {"type": "string"},
    "Symptoms today- Headache": {"type": "string"},
    "Symptoms today- Nausea/vomiting": {"type": "string"},
    "Symptoms today- Abdominal pain": {"type": "string"},
    "Symptoms today- Diarrhoea": {"type": "string"},
    "Symptoms today- Loss of taste": {"type": "string"},
    "Symptoms today- Loss of smell": {"type": "string"},
    "Are you self Isolating?": {"type": "string"},
    "Received shielding letter from NHS?": {"type": "string"},
    "Do you think you have Covid Symptoms?": {"type": "string"},
    "Contact with Known Positive COVID19 Case": {"type": "string"},
    "If Known; Last contact date": {"type": "string"},
    "If Known; Type of contact": {"type": "string"},
    "Contact with Suspected Covid19 Case": {"type": "string"},
    "If Suspect; Last contact date": {"type": "string"},
    "If Suspect; Type of contact": {"type": "string"},
    "Household been Hospital last 2 wks": {"type": "string"},
    "Household been in Care home last 2 wks": {"type": "string"},
    "Do you think you have had Covid 19?": {"type": "string"},
    "Which symptoms did you have?": {"type": "string"},
    "Previous Symptoms-Fever": {"type": "string"},
    "Previous Symptoms-Muscle ache (myalgia)": {"type": "string"},
    "Previous Symptoms-Fatigue (weakness)": {"type": "string"},
    "Previous Symptoms-Sore throat": {"type": "string"},
    "Previous Symptoms-Cough": {"type": "string"},
    "Previous Symptoms-Shortness of breath": {"type": "string"},
    "Previous Symptoms-Headache": {"type": "string"},
    "Previous Symptoms-Nausea/vomiting": {"type": "string"},
    "Previous Symptoms-Abdominal pain": {"type": "string"},
    "Previous Symptoms-Diarrhoea": {"type": "string"},
    "Previous Symptoms-Loss of taste": {"type": "string"},
    "Previous Symptoms-Loss of smell": {"type": "string"},
    "If Yes; Date of first symptoms": {"type": "string"},
    "Did you contact NHS?": {"type": "string"},
    "If Yes; Were you tested": {"type": "string"},
    "If Yes;Test Result": {"type": "string"},
    "Were you admitted to hospital?": {"type": "string"},
}
survey_responses_v1_validation_schema = {
    "ons_household_id": {"type": "string"},
    "Visit_ID": {"type": "integer"},
    "Type_of_Visit": {"type": "string"},
    "Participant_Visit_status": {"type": "string"},
    "Withdrawal_reason": {"type": "string"},
    "Visit_Date_Time": {"type": "string"},
    "Street": {"type": "string"},
    "City": {"type": "string"},
    "County": {"type": "string"},
    "Postcode": {"type": "string"},
    "Cohort": {"type": "string"},
    "No_Paticicpants_not_Consented": {"type": "integer"},
    "Reason_Participants_not_Consented": {"type": "string"},
    "No_Participants_not_present_for_Visit": {"type": "integer"},
    "Reason_Participants_not_present_on_Visit": {"type": "string"},
    "Household_Members_Under_2_Years": {"type": "string"},
    "Infant_1": {"type": "string"},
    "Infant_2": {"type": "string"},
    "Infant_3": {"type": "string"},
    "Infant_4": {"type": "string"},
    "Infant_5": {"type": "string"},
    "Infant_6": {"type": "string"},
    "Infant_7": {"type": "string"},
    "Infant_8": {"type": "string"},
    "Household_Members_Over_2_and_Not_Present": {"type": "string"},
    "Person_1": {"type": "string"},
    "Person_2": {"type": "string"},
    "Person_3": {"type": "string"},
    "Person_4": {"type": "string"},
    "Person_5": {"type": "string"},
    "Person_6": {"type": "string"},
    "Person_7": {"type": "string"},
    "Person_8": {"type": "string"},
    "Person_1_Not_Consenting_Age": {"type": "integer"},
    "Person1_Reason_for_Not_Consenting": {"type": "string"},
    "Person_2_Not_Consenting_Age": {"type": "integer"},
    "Person2_Reason_for_Not_Consenting": {"type": "string"},
    "Person_3_Not_Consenting_Age": {"type": "integer"},
    "Person3_Reason_for_Not_Consenting": {"type": "string"},
    "Person_4_Not_Consenting_Age": {"type": "integer"},
    "Person4_Reason_for_Not_Consenting": {"type": "string"},
    "Person_5_Not_Consenting_Age": {"type": "integer"},
    "Person5_Reason_for_Not_Consenting": {"type": "string"},
    "Person_6_Not_Consenting_Age": {"type": "integer"},
    "Person6_Reason_for_Not_Consenting": {"type": "string"},
    "Person_7_Not_Consenting_Age": {"type": "integer"},
    "Person7_Reason_for_Not_Consenting": {"type": "string"},
    "Person_8_Not_Consenting_Age": {"type": "integer"},
    "Person8_Reason_for_Not_Consenting": {"type": "string"},
    "Person_9_Not_Consenting_Age": {"type": "integer"},
    "Person9_Reason_for_Not_Consenting": {"type": "string"},
    "Participant_id": {"type": "string"},
    "Title": {"type": "string"},
    "First_Name": {"type": "string"},
    "Last_Name": {"type": "string"},
    "DoB": {"type": "string"},
    "Email": {"type": "string"},
    "No_Email_address": {"type": "integer"},
    "Bloods_Taken": {"type": "string"},
    "bloods_barcode_1": {"type": "string"},
    "Swab_Taken": {"type": "string"},
    "Swab_Barcode_1": {"type": "string"},
    "Date_Time_Samples_Taken": {"type": "string"},
    "Sex": {"type": "string"},
    "Gender": {"type": "string"},
    "Ethnicity": {"type": "string"},
    "Ethnicity_Other": {"type": "string"},
    "Consent_to_First_Visit": {"type": "string"},
    "Consent_to_Five_Visits": {"type": "string"},
    "Consent_to_Sixteen_Visits": {"type": "string"},
    "Consent_to_Blood_Test": {"type": "string"},
    "Consent_to_be_Contacted_Extra_Research": {"type": "string"},
    "Consent_to_use_of_Surplus_Blood_Samples": {"type": "string"},
    "What_is_the_title_of_your_main_job": {"type": "string"},
    "What_do_you_do_in_your_main_job_business": {"type": "string"},
    "Occupations_sectors_do_you_work_in": {"type": "string"},
    "occupation_sector_other": {"type": "string"},
    "Work_in_a_nursing_residential_care_home": {"type": "string"},
    "Do_you_currently_work_in_healthcare": {"type": "string"},
    "Direct_contact_patients_clients_resid": {"type": "string"},
    "Have_physical_mental_health_or_illnesses": {"type": "string"},
    "physical_mental_health_or_illness_reduces_activity_ability": {"type": "string"},
    "Have_you_ever_smoked_regularly": {"type": "string"},
    "Do_you_currently_smoke_or_vape": {"type": "string"},
    "Smoke_Yes_cigarettes": {"type": "string"},
    "Smoke_Yes_cigar": {"type": "string"},
    "Smoke_Yes_pipe": {"type": "string"},
    "Smoke_Yes_vape_e_cigarettes": {"type": "string"},
    "Smoke_No": {"type": "string"},
    "Smoke_Hookah_shisha pipes": {"type": "string"},
    "What_is_your_current_working_status": {"type": "string"},
    "Where_are_you_mainly_working_now": {"type": "string"},
    "How_often_do_you_work_elsewhere": {"type": "string"},
    "Can_you_socially_distance_at_work": {"type": "string"},
    "How_do_you_get_to_and_from_work_school": {"type": "string"},
    "Had_symptoms_in_the_last_7_days": {"type": "string"},
    "Which_symptoms_in_the_last_7_days": {"type": "string"},
    "Date_of_first_symptom_onset": {"type": "string"},
    "Symptoms_7_Fever": {"type": "string"},
    "Symptoms_7_Muscle_ache_myalgia": {"type": "string"},
    "Symptoms_7_Fatigue_weakness": {"type": "string"},
    "Symptoms_7_Sore_throat": {"type": "string"},
    "Symptoms_7_Cough": {"type": "string"},
    "Symptoms_7_Shortness_of_breath": {"type": "string"},
    "Symptoms_7_Headache": {"type": "string"},
    "Symptoms_7_Nausea_vomiting": {"type": "string"},
    "Symptoms_7_Abdominal_pain": {"type": "string"},
    "Symptoms_7_Diarrhoea": {"type": "string"},
    "Symptoms_7_Loss_of_taste": {"type": "string"},
    "Symptoms_7_Loss_of_smell": {"type": "string"},
    "Are_you_self_Isolating_S2": {"type": "string"},
    "Do_you_think_you_have_Covid_Symptoms": {"type": "string"},
    "Contact_Known_Positive_COVID19_28_days": {"type": "string"},
    "If_Known_last_contact_date": {"type": "string"},
    "If_Known_type_of_contact_S2": {"type": "string"},
    "Contact_Suspect_Positive_COVID19_28_d": {"type": "string"},
    "If_suspect_last_contact_date": {"type": "string"},
    "If_suspect_type_of_contact_S2": {"type": "string"},
    "Household_been_Hospital_last_28_days": {"type": "string"},
    "Household_been_in_Care_Home_last_28_days": {"type": "string"},
    "Hours_a_day_with_someone_else": {"type": "integer"},
    "Physical_Contact_18yrs": {"type": "string"},
    "Physical_Contact_18_to_69_yrs": {"type": "string"},
    "Physical_Contact_70_yrs": {"type": "string"},
    "Social_Distance_Contact_18yrs": {"type": "string"},
    "Social_Distance_Contact_18_to_69_yrs": {"type": "string"},
    "Social_Distance_Contact_70_yrs": {"type": "string"},
    "1Hour_or_Longer_another_person_home": {"type": "string"},
    "1Hour_or_Longer_another_person_yourhome": {"type": "string"},
    "Times_Outside_Home_For_Shopping": {"type": "string"},
    "Face_Covering_or_Mask_outside_of_home": {"type": "string"},
    "Do_you_think_you_have_had_Covid_19": {"type": "string"},
    "think_had_covid_19_any_symptoms": {"type": "string"},
    "think_had_covid_19_which_symptoms": {"type": "string"},
    "Previous_Symptoms_Fever": {"type": "string"},
    "Previous_Symptoms_Muscle_ache_myalgia": {"type": "string"},
    "Previous_Symptoms_Fatigue_weakness": {"type": "string"},
    "Previous_Symptoms_Sore_throat": {"type": "string"},
    "Previous_Symptoms_Cough": {"type": "string"},
    "Previous_Symptoms_Shortness_of_breath": {"type": "string"},
    "Previous_Symptoms_Headache": {"type": "string"},
    "Previous_Symptoms_Nausea_vomiting": {"type": "string"},
    "Previous_Symptoms_Abdominal_pain": {"type": "string"},
    "Previous_Symptoms_Diarrhoea": {"type": "string"},
    "Previous_Symptoms_Loss_of_taste": {"type": "string"},
    "Previous_Symptoms_Loss_of_smell": {"type": "string"},
    "If_yes_Date_of_first_symptoms": {"type": "string"},
    "Did_you_contact_NHS": {"type": "string"},
    "Were_you_admitted_to_hospital": {"type": "string"},
    "Have_you_had_a_swab_test": {"type": "string"},
    "If_Yes_What_was_result": {"type": "string"},
    "If_positive_Date_of_1st_ve_test": {"type": "string"},
    "If_all_negative_Date_last_test": {"type": "string"},
    "Have_you_had_a_blood_test_for_Covid": {"type": "string"},
    "What_was_the_result_of_the_blood_test": {"type": "string"},
    "Where_was_the_test_done": {"type": "string"},
    "If_ve_Blood_Date_of_1st_ve_test": {"type": "string"},
    "If_all_ve_blood_Date_last_ve_test": {"type": "string"},
    "Have_you_been_outside_UK_since_April": {"type": "string"},
    "been_outside_uk_last_country": {"type": "string"},
    "been_outside_uk_last_date": {"type": "string"},
    "Vaccinated_Against_Covid": {"type": "string"},
    "Date_Of_Vaccination": {"type": "string"},
}
survey_responses_v2_validation_schema = {
    "ons_household_id": {"type": "string"},
    "Visit_ID": {"type": "string"},
    "Visit Status": {"type": "string"},
    "Participant_Visit_status": {"type": "string"},
    "Participant_status": {"type": "string"},
    "Withdrawal_reason": {"type": "string"},
    "Type_of_Visit": {"type": "string"},
    "Visit_Order": {"type": "string"},
    "Work_Type_Picklist": {"type": "string"},
    "Visit_Date_Time": {"type": "string"},
    "Street": {"type": "string"},
    "City": {"type": "string"},
    "County": {"type": "string"},
    "Postcode": {"type": "string"},
    "Cohort": {"type": "string"},
    "Fingerprick_Status": {"type": "string"},
    "Household_Members_Under_2_Years": {"type": "string"},
    "Infant_1": {"type": "integer"},
    "Infant_2": {"type": "integer"},
    "Infant_3": {"type": "integer"},
    "Infant_4": {"type": "integer"},
    "Infant_5": {"type": "integer"},
    "Infant_6": {"type": "integer"},
    "Infant_7": {"type": "integer"},
    "Infant_8": {"type": "integer"},
    "Household_Members_Over_2_and_Not_Present": {"type": "string"},
    "Person_1": {"type": "integer"},
    "Person_2": {"type": "integer"},
    "Person_3": {"type": "integer"},
    "Person_4": {"type": "integer"},
    "Person_5": {"type": "integer"},
    "Person_6": {"type": "integer"},
    "Person_7": {"type": "integer"},
    "Person_8": {"type": "integer"},
    "Person_1_Not_Consenting_Age": {"type": "integer"},
    "Person1_Reason_for_Not_Consenting": {"type": "string"},
    "Person_2_Not_Consenting_Age": {"type": "integer"},
    "Person2_Reason_for_Not_Consenting": {"type": "string"},
    "Person_3_Not_Consenting_Age": {"type": "integer"},
    "Person3_Reason_for_Not_Consenting": {"type": "string"},
    "Person_4_Not_Consenting_Age": {"type": "integer"},
    "Person4_Reason_for_Not_Consenting": {"type": "string"},
    "Person_5_Not_Consenting_Age": {"type": "integer"},
    "Person5_Reason_for_Not_Consenting": {"type": "string"},
    "Person_6_Not_Consenting_Age": {"type": "integer"},
    "Person6_Reason_for_Not_Consenting": {"type": "string"},
    "Person_7_Not_Consenting_Age": {"type": "integer"},
    "Person7_Reason_for_Not_Consenting": {"type": "string"},
    "Person_8_Not_Consenting_Age": {"type": "integer"},
    "Person8_Reason_for_Not_Consenting": {"type": "string"},
    "Person_9_Not_Consenting_Age": {"type": "integer"},
    "Person9_Reason_for_Not_Consenting": {"type": "string"},
    "Participant_id": {"type": "string"},
    "Title": {"type": "string"},
    "First_Name": {"type": "string"},
    "Middle_Name": {"type": "string"},
    "Last_Name": {"type": "string"},
    "DoB": {"type": "string"},
    "Email": {"type": "string"},
    "Have_landline_number": {"type": "string"},
    "Have_mobile_number": {"type": "string"},
    "Have_email_address": {"type": "string"},
    "Prefer_receive_vouchers": {"type": "string"},
    "Confirm_receive_vouchers": {"type": "string"},
    "No_Email_address": {"type": "integer"},
    "Able_to_take_blood": {"type": "string"},
    "No_Blood_reason_fingerprick": {"type": "string"},
    "No_Blood_reason_venous": {"type": "string"},
    "bloods_barcode_1": {"type": "string"},
    "Swab_Barcode_1": {"type": "string"},
    "Date_Time_Samples_Taken": {"type": "string"},
    "Sex": {"type": "string"},
    "Gender": {"type": "string"},
    "Ethnic_group": {"type": "string"},
    "Ethnicity": {"type": "string"},
    "Ethnicity_Other": {"type": "string"},
    "Consent_to_First_Visit": {"type": "string"},
    "Consent_to_Five_Visits": {"type": "string"},
    "Consent_to_April_22": {"type": "string"},
    "Consent_to_Sixteen_Visits": {"type": "string"},
    "Consent_to_Blood_Test": {"type": "string"},
    "Consent_to_Finger_prick_A1_A3": {"type": "string"},
    "Consent_to_extend_study_under_16_B1_B3": {"type": "string"},
    "Consent_to_be_Contacted_Extra_Research": {"type": "string"},
    "Consent_to_be_Contacted_Extra_ResearchYN": {"type": "string"},
    "Consent_to_use_of_Surplus_Blood_Samples": {"type": "string"},
    "Consent_to_use_of_Surplus_Blood_SamplesYN": {"type": "string"},
    "Approached_for_blood_samples?": {"type": "string"},
    "Consent_to_blood_samples_if_positive": {"type": "string"},
    "Consent_to_blood_samples_if_positiveYN": {"type": "string"},
    "Consent_to_fingerprick_blood_samples": {"type": "string"},
    "Accepted_invite_to_fingerprick": {"type": "string"},
    "Re_consented_for_blood": {"type": "string"},
    "What_is_the_title_of_your_main_job": {"type": "string"},
    "What_do_you_do_in_your_main_job_business": {"type": "string"},
    "Occupations_sectors_do_you_work_in": {"type": "string"},
    "occupation_sector_other": {"type": "string"},
    "Work_in_a_nursing_residential_care_home": {"type": "string"},
    "Do_you_currently_work_in_healthcare": {"type": "string"},
    "Direct_contact_patients_clients_resid": {"type": "string"},
    "Have_physical_mental_health_or_illnesses": {"type": "string"},
    "physical_mental_health_or_illness_reduces_activity_ability": {"type": "string"},
    "Have_you_ever_smoked_regularly": {"type": "string"},
    "Do_you_currently_smoke_or_vape": {"type": "string"},
    "Do_you_currently_smoke_or_vape_at_all": {"type": "string"},
    "Smoke_Yes_cigarettes": {"type": "string"},
    "Smoke_Yes_cigar": {"type": "string"},
    "Smoke_Yes_pipe": {"type": "string"},
    "Smoke_Yes_vape_e_cigarettes": {"type": "string"},
    "Smoke_Hookah/shisha pipes": {"type": "string"},
    "What_is_your_current_working_status": {"type": "string"},
    "Paid_employment": {"type": "string"},
    "Main_Job_Changed": {"type": "string"},
    "Where_are_you_mainly_working_now": {"type": "string"},
    "How_often_do_you_work_elsewhere": {"type": "string"},
    "How_do_you_get_to_and_from_work_school": {"type": "string"},
    "Can_you_socially_distance_at_work": {"type": "string"},
    "Had_symptoms_in_the_last_7_days": {"type": "string"},
    "Which_symptoms_in_the_last_7_days": {"type": "string"},
    "Date_of_first_symptom_onset": {"type": "string"},
    "Symptoms_7_Fever": {"type": "string"},
    "Symptoms_7_Muscle_ache_myalgia": {"type": "string"},
    "Symptoms_7_Fatigue_weakness": {"type": "string"},
    "Symptoms_7_Sore_throat": {"type": "string"},
    "Symptoms_7_Cough": {"type": "string"},
    "Symptoms_7_Shortness_of_breath": {"type": "string"},
    "Symptoms_7_Headache": {"type": "string"},
    "Symptoms_7_Nausea_vomiting": {"type": "string"},
    "Symptoms_7_Abdominal_pain": {"type": "string"},
    "Symptoms_7_Diarrhoea": {"type": "string"},
    "Symptoms_7_Loss_of_taste": {"type": "string"},
    "Symptoms_7_Loss_of_smell": {"type": "string"},
    "Are_you_self_Isolating_S2": {"type": "string"},
    "Do_you_think_you_have_Covid_Symptoms": {"type": "string"},
    "Contact_Known_Positive_COVID19_28_days": {"type": "string"},
    "If_Known_last_contact_date": {"type": "string"},
    "If_Known_type_of_contact_S2": {"type": "string"},
    "Contact_Suspect_Positive_COVID19_28_d": {"type": "string"},
    "If_suspect_last_contact_date": {"type": "string"},
    "If_suspect_type_of_contact_S2": {"type": "string"},
    "You_been_Hospital_last_28_days": {"type": "string"},
    "OtherHouse_been_Hospital_last_28_days": {"type": "string"},
    "Your_been_in_Care_Home_last_28_days": {"type": "string"},
    "OtherHouse_been_in_Care_Home_last_28_days": {"type": "string"},
    "Hours_a_day_with_someone_else": {"type": "string"},
    "Physical_Contact_18yrs": {"type": "string"},
    "Physical_Contact_18_to_69_yrs": {"type": "string"},
    "Physical_Contact_70_yrs": {"type": "string"},
    "Social_Distance_Contact_18yrs": {"type": "string"},
    "Social_Distance_Contact_18_to_69_yrs": {"type": "string"},
    "Social_Distance_Contact_70_yrs": {"type": "string"},
    "1Hour_or_Longer_another_person_home": {"type": "string"},
    "1Hour_or_Longer_another_person_yourhome": {"type": "string"},
    "Times_Outside_Home_For_Shopping": {"type": "string"},
    "Shopping_last_7_days": {"type": "string"},
    "Socialise_last_7_days": {"type": "string"},
    "Regular_testing_COVID": {"type": "string"},
    "Face_Covering_or_Mask_outside_of_home": {"type": "string"},
    "Face_Mask_Work_Place": {"type": "string"},
    "Face_Mask_Other_Enclosed_Places": {"type": "string"},
    "Do_you_think_you_have_had_Covid_19": {"type": "string"},
    "think_had_covid_19_any_symptoms": {"type": "string"},
    "think_had_covid_19_which_symptoms": {"type": "string"},
    "Previous_Symptoms_Fever": {"type": "string"},
    "Previous_Symptoms_Muscle_ache_myalgia": {"type": "string"},
    "Previous_Symptoms_Fatigue_weakness": {"type": "string"},
    "Previous_Symptoms_Sore_throat": {"type": "string"},
    "Previous_Symptoms_Cough": {"type": "string"},
    "Previous_Symptoms_Shortness_of_breath": {"type": "string"},
    "Previous_Symptoms_Headache": {"type": "string"},
    "Previous_Symptoms_Nausea_vomiting": {"type": "string"},
    "Previous_Symptoms_Abdominal_pain": {"type": "string"},
    "Previous_Symptoms_Diarrhoea": {"type": "string"},
    "Previous_Symptoms_Loss_of_taste": {"type": "string"},
    "Previous_Symptoms_Loss_of_smell": {"type": "string"},
    "If_yes_Date_of_first_symptoms": {"type": "string"},
    "Did_you_contact_NHS": {"type": "string"},
    "Were_you_admitted_to_hospital": {"type": "string"},
    "Have_you_had_a_swab_test": {"type": "string"},
    "If_Yes_What_was_result": {"type": "string"},
    "If_positive_Date_of_1st_ve_test": {"type": "string"},
    "If_all_negative_Date_last_test": {"type": "string"},
    "Have_you_had_a_blood_test_for_Covid": {"type": "string"},
    "What_was_the_result_of_the_blood_test": {"type": "string"},
    "Where_was_the_test_done": {"type": "string"},
    "If_ve_Blood_Date_of_1st_ve_test": {"type": "string"},
    "If_all_ve_blood_Date_last_ve_test": {"type": "string"},
    "Have_Long_Covid_Symptoms": {"type": "string"},
    "Long_Covid_Reduce_Activities": {"type": "string"},
    "Long_Covid_Symptoms": {"type": "string"},
    "Long_Covid_Fever": {"type": "string"},
    "Long_Covid_Weakness_tiredness": {"type": "string"},
    "Long_Covid_Diarrhoea": {"type": "string"},
    "Long_Covid_Loss_of_smell": {"type": "string"},
    "Long_Covid_Shortness_of_breath": {"type": "string"},
    "Long_Covid_Vertigo_dizziness": {"type": "string"},
    "Long_Covid_Trouble_sleeping": {"type": "string"},
    "Long_Covid_Headache": {"type": "string"},
    "Long_Covid_Nausea_vomiting": {"type": "string"},
    "Long_Covid_Loss_of_appetite": {"type": "string"},
    "Long_Covid_Sore_throat": {"type": "string"},
    "Long_Covid_Chest_pain": {"type": "string"},
    "Long_Covid_Worry_anxiety": {"type": "string"},
    "Long_Covid_Memory_loss_or_confusion": {"type": "string"},
    "Long_Covid_Muscle_ache": {"type": "string"},
    "Long_Covid_Abdominal_pain": {"type": "string"},
    "Long_Covid_Loss_of_taste": {"type": "string"},
    "Long_Covid_Cough": {"type": "string"},
    "Long_Covid_Palpitations": {"type": "string"},
    "Long_Covid_Low_mood_not_enjoying_anything": {"type": "string"},
    "Long_Covid_Difficulty_concentrating": {"type": "string"},
    "Have_you_been_offered_a_vaccination": {"type": "string"},
    "Vaccinated_Against_Covid": {"type": "string"},
    "Type_Of_Vaccination": {"type": "string"},
    "Vaccination_Other": {"type": "string"},
    "Number_Of_Doses": {"type": "string"},
    "Date_Of_Vaccination": {"type": "string"},
    "Have_you_been_outside_UK_since_April": {"type": "string"},
    "been_outside_uk_last_country": {"type": "string"},
    "been_outside_uk_last_date": {"type": "string"},
    "Have_you_been_outside_UK_Lastspoke": {"type": "string"},
}
