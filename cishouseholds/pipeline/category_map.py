_times_in_last_7_day_categories = {
    "None": 0,
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "7 times or more": 7,
}

_yes_no_categories = {"No": 0, "Yes": 1}

category_maps = {
    "iqvia_raw_category_map": {
        "agreed_to_additional_consent_visit": _yes_no_categories,
        "consent_blood_samples_if_positive_yn": _yes_no_categories,
        "consent_contact_extra_research_yn": _yes_no_categories,
        "consent_finger_prick_a1_a3": _yes_no_categories,
        "consent_blood_samples_if_positive_yn": _yes_no_categories,
        "consent_extend_study_under_16_b1_b3": _yes_no_categories,
        "consent_fingerprick_blood_samples": {"false": 0, "true": 1},
        "reconsented_blood": {"false": 0, "true": 1},
        "consent_extend_study_under_16_b1_b3": _yes_no_categories,
        "consent_use_of_surplus_blood_samples_yn": _yes_no_categories,
        "consent_blood_samples_if_positive": {"false": 0, "true": 1},
        "swab_taken": _yes_no_categories,
        "blood_taken": _yes_no_categories,
        "illness_lasting_over_12_months": _yes_no_categories,
        "ever_smoked_regularly": _yes_no_categories,
        "smoke_cigarettes": _yes_no_categories,
        "smokes_cigar": _yes_no_categories,
        "smokes_pipe": _yes_no_categories,
        "smokes_vape_e_cigarettes": _yes_no_categories,
        "smokes_hookah_shisha_pipes": _yes_no_categories,
        "smokes_nothing_now": _yes_no_categories,
        "work_nursing_or_residential_care_home": _yes_no_categories,
        "work_direct_contact_patients_or_clients": _yes_no_categories,
        "think_have_covid_symptom_any": _yes_no_categories,
        "think_have_covid_symptom_fever": _yes_no_categories,
        "think_have_covid_symptom_muscle_ache": _yes_no_categories,
        "think_have_covid_symptom_fatigue": _yes_no_categories,
        "think_have_covid_symptom_sore_throat": _yes_no_categories,
        "think_have_covid_symptom_cough": _yes_no_categories,
        "think_have_covid_symptom_shortness_of_breath": _yes_no_categories,
        "think_have_covid_symptom_headache": _yes_no_categories,
        "think_have_covid_symptom_nausea_or_vomiting": _yes_no_categories,
        "think_have_covid_symptom_abdominal_pain": _yes_no_categories,
        "think_have_covid_symptom_diarrhoea": _yes_no_categories,
        "think_have_covid_symptom_loss_of_taste": _yes_no_categories,
        "think_have_covid_symptom_loss_of_smell": _yes_no_categories,
        "think_have_covid_symptom_more_trouble_sleeping": _yes_no_categories,
        "think_have_covid_symptom_runny_nose_or_sneezing": _yes_no_categories,
        "think_have_covid_symptom_noisy_breathing": _yes_no_categories,
        "think_have_covid_symptom_loss_of_appetite": _yes_no_categories,
        "think_have_covid_symptom_chest_pain": _yes_no_categories,
        "think_have_covid_symptom_palpitations": _yes_no_categories,
        "think_have_covid_symptom_vertigo_or_dizziness": _yes_no_categories,
        "think_have_covid_symptom_anxiety": _yes_no_categories,
        "think_have_covid_symptom_low_mood": _yes_no_categories,
        "think_have_covid_symptom_memory_loss_or_confusion": _yes_no_categories,
        "think_have_covid_symptom_difficulty_concentrating": _yes_no_categories,
        "think_have_covid": _yes_no_categories,
        "self_isolating": _yes_no_categories,
        "received_shielding_letter": _yes_no_categories,
        "contact_known_positive_covid_last_28_days": _yes_no_categories,
        "contact_suspected_positive_covid_last_28_days": _yes_no_categories,
        "hospital_last_28_days": _yes_no_categories,
        "other_household_member_hospital_last_28_days": _yes_no_categories,
        "care_home_last_28_days": _yes_no_categories,
        "other_household_member_care_home_last_28_days": _yes_no_categories,
        "think_had_covid": _yes_no_categories,
        "think_had_covid_contacted_nhs": _yes_no_categories,
        "think_had_covid_admitted_to_hospital": _yes_no_categories,
        "think_had_covid_any_symptoms": _yes_no_categories,
        "think_had_covid_symptom_fever": _yes_no_categories,
        "think_had_covid_symptom_muscle_ache": _yes_no_categories,
        "think_had_covid_symptom_fatigue": _yes_no_categories,
        "think_had_covid_symptom_sore_throat": _yes_no_categories,
        "think_had_covid_symptom_cough": _yes_no_categories,
        "think_had_covid_symptom_shortness_of_breath": _yes_no_categories,
        "think_had_covid_symptom_headache": _yes_no_categories,
        "think_had_covid_symptom_nausea_or_vomiting": _yes_no_categories,
        "think_had_covid_symptom_abdominal_pain": _yes_no_categories,
        "think_had_covid_symptom_diarrhoea": _yes_no_categories,
        "think_had_covid_symptom_loss_of_taste": _yes_no_categories,
        "think_had_covid_symptom_loss_of_smell": _yes_no_categories,
        "think_had_covid_symptom_more_trouble_sleeping": _yes_no_categories,
        "think_had_covid_symptom_runny_nose_or_sneezing": _yes_no_categories,
        "think_had_covid_symptom_noisy_breathing": _yes_no_categories,
        "think_had_covid_symptom_loss_of_appetite": _yes_no_categories,
        "think_had_covid_symptom_chest_pain": _yes_no_categories,
        "think_had_covid_symptom_palpitations": _yes_no_categories,
        "think_had_covid_symptom_vertigo_or_dizziness": _yes_no_categories,
        "think_had_covid_symptom_anxiety": _yes_no_categories,
        "think_had_covid_symptom_low_mood": _yes_no_categories,
        "think_had_covid_symptom_memory_loss_or_confusion": _yes_no_categories,
        "think_had_covid_symptom_difficulty_concentrating": _yes_no_categories,
        "think_had_covid_symptom_low_mood": _yes_no_categories,
        "other_covid_infection_test": _yes_no_categories,
        "other_antibody_test": _yes_no_categories,
        "been_outside_uk": _yes_no_categories,
        "think_have_long_covid": _yes_no_categories,
        "think_have_long_covid_symptom_fever": _yes_no_categories,
        "think_have_long_covid_symptom_fatigue": _yes_no_categories,
        "think_have_long_covid_symptom_diarrhoea": _yes_no_categories,
        "think_have_long_covid_symptom_loss_of_smell": _yes_no_categories,
        "think_have_long_covid_symptom_shortness_of_breath": _yes_no_categories,
        "think_have_long_covid_symptom_vertigo_or_dizziness": _yes_no_categories,
        "think_have_long_covid_symptom_more_trouble_sleeping": _yes_no_categories,
        "think_have_long_covid_symptom_headache": _yes_no_categories,
        "think_have_long_covid_symptom_nausea_or_vomiting": _yes_no_categories,
        "think_have_long_covid_symptom_loss_of_appetite": _yes_no_categories,
        "think_have_long_covid_symptom_sore_throat": _yes_no_categories,
        "think_have_long_covid_symptom_chest_pain": _yes_no_categories,
        "think_have_long_covid_symptom_anxiety": _yes_no_categories,
        "think_have_long_covid_symptom_memory_loss_or_confusion": _yes_no_categories,
        "think_have_long_covid_symptom_muscle_ache": _yes_no_categories,
        "think_have_long_covid_symptom_abdominal_pain": _yes_no_categories,
        "think_have_long_covid_symptom_loss_of_taste": _yes_no_categories,
        "think_have_long_covid_symptom_cough": _yes_no_categories,
        "think_have_long_covid_symptom_palpitations": _yes_no_categories,
        "think_have_long_covid_symptom_low_mood": _yes_no_categories,
        "think_have_long_covid_symptom_difficulty_concentrating": _yes_no_categories,
        "think_have_long_covid_symptom_runny_nose_or_sneezing": _yes_no_categories,
        "think_have_long_covid_symptom_noisy_breathing": _yes_no_categories,
        "confirm_received_vouchers": _yes_no_categories,
        "have_landline_number": _yes_no_categories,
        "have_mobile_number": _yes_no_categories,
        "have_email_address": _yes_no_categories,
        "work_main_job_changed": _yes_no_categories,
        "work_in_additional_paid_employment": _yes_no_categories,
        "cis_covid_vaccine_received": _yes_no_categories,
        "cis_flu_vaccine_received": _yes_no_categories,
        "did_not_attend_inferred": _yes_no_categories,
        "participant_visit_status": {
            "Cancelled": 0,
            "Completed": 1,
            "Patient did not attend": 2,
            "Participant did not attend": 2,
            "Re-scheduled": 3,
            "Scheduled": 4,
            "Partially Completed": 5,
            "Withdrawn": 6,
            "New": 7,
            "Dispatched": 8,
            "Household did not attend": 9,
        },
        "survey_response_type": {"First Visit": 0, "Follow-up Visit": 1},
        "sex": {"Male": 1, "Female": 2},
        "ethnicity": {
            "White-British": 1,
            "White-Irish": 2,
            "White-Gypsy or Irish Traveller": 3,
            "Any other white background": 4,
            "Mixed-White & Black Caribbean": 5,
            "Mixed-White & Black African": 6,
            "Mixed-White & Asian": 7,
            "Any other Mixed background": 8,
            "Asian or Asian British-Indian": 9,
            "Asian or Asian British-Pakistani": 10,
            "Asian or Asian British-Bangladeshi": 11,
            "Asian or Asian British-Chinese": 12,
            "Any other Asian background": 13,
            "Black,Caribbean,African-African": 14,
            "Black,Caribbean,Afro-Caribbean": 15,
            "Any other Black background": 16,
            "Other ethnic group-Arab": 17,
            "Any other ethnic group": 18,
        },
        "ethnicity_white": {
            "Non-White": 0,
            "White": 1,
        },
        "illness_reduces_activity_or_ability": {"Not at all": 0, "Yes, a little": 1, "Yes, a lot": 2},
        "work_sector": {
            "Teaching and education": 1,
            "Health care": 2,
            "Social care": 3,
            "Transport (incl. storage, logistic)": 4,
            "Retail sector (incl. wholesale)": 5,
            "Hospitality (e.g. hotel, restaurant)": 6,
            "Food production, agriculture, farming": 7,
            "Personal services (e.g. hairdressers)": 8,
            "Information technology and communication": 9,
            "Financial services incl. insurance": 10,
            "Manufacturing or construction": 11,
            "Civil service or Local Government": 12,
            "Armed forces": 13,
            "Arts,Entertainment or Recreation": 14,
            "Other occupation sector": 15,
            "NA(Not currently working)": 99,
        },
        "work_health_care_area": {
            "No": 0,
            "Yes, in primary care, e.g. GP, dentist": 1,
            "Yes, in secondary care, e.g. hospital": 2,
            "Yes, in other healthcare settings, e.g. mental health": 3,
        },
        "work_health_care_patient_facing": {
            "No": 0,
            "Yes, primary care, patient-facing": 1,
            "Yes, secondary care, patient-facing": 2,
            "Yes, other healthcare, patient-facing": 3,
            "Yes, primary care, non-patient-facing": 4,
            "Yes, secondary care, non-patient-facing": 5,
            "Yes, other healthcare, non-patient-facing": 6,
        },
        "work_social_care": {
            "No": 0,
            "Yes, care/residential home, resident-facing": 1,
            "Yes, other social care, resident-facing": 2,
            "Yes, care/residential home, non-resident-facing": 3,
            "Yes, other social care, non-resident-facing": 4,
        },
        "work_status_v0": {
            "Employed": 1,
            "Self-employed": 2,
            "Furloughed (temporarily not working)": 3,
            "Not working (unemployed, retired, long-term sick etc.)": 4,
            "Student": 5,
        },
        "work_status_v1": {
            "Employed and currently working": 1,
            "Employed and currently not working": 2,
            "Self-employed and currently working": 3,
            "Self-employed and currently not working": 4,
            "Looking for paid work and able to start": 5,
            "Not working and not looking for work": 6,
            "Retired": 7,
            "Child under 5y not attending child care": 8,
            "Child under 5y attending child care": 9,
            "5y and older in full-time education": 10,
        },
        "work_status_v2": {
            "Employed and currently working": 1,
            "Employed and currently not working": 2,
            "Self-employed and currently working": 3,
            "Self-employed and currently not working": 4,
            "Looking for paid work and able to start": 5,
            "Not working and not looking for work": 6,
            "Retired": 7,
            "Child under 4-5y not attending child care": 8,
            "Child under 4-5y attending child care": 9,
            "4-5y and older at school/home-school": 10,
            "Attending college or FE (including if temporarily absent)": 11,
            "Attending university (including if temporarily absent)": 12,
        },
        "work_location": {
            "Working from home": 1,
            "Working somewhere else (not your home)": 2,
            "Both (from home and somewhere else)": 3,
            "Not applicable, not currently working": 4,
        },
        "ability_to_socially_distance_at_work_or_education": {
            "Easy to maintain 2m": 1,
            "Relatively easy to maintain 2m": 2,
            "Difficult to maintain 2m, but can be 1m": 3,
            "Very difficult to be more than 1m away": 4,
            "N/A (not working/in education etc)": 9,
        },
        "transport_to_work_or_education": {
            "Underground, metro, light rail, tram": 1,
            "Train": 2,
            "Bus, minibus, coach": 3,
            "Motorbike, scooter or moped": 4,
            "Car or van": 5,
            "Taxi/minicab": 6,
            "Bicycle": 7,
            "On foot": 8,
            "Other method": 9,
            "N/A (not working/in education etc)": 99,
        },
        "self_isolating_reason": {
            "No": 0,
            "Yes, you have/have had symptoms": 1,
            "Yes, someone you live with had symptoms": 2,
            "Yes, for other reasons (e.g. going into hospital, quarantining)": 3,
        },
        "last_covid_contact_type": {"Living in your own home": 1, "Outside your home": 2},
        "last_suspected_covid_contact_type": {"Living in your own home": 1, "Outside your home": 2},
        "household_been_hospital_last_28_days": {
            "No, no one in my household has": 0,
            "Yes, I have": 1,
            "No I haven’t, but someone else in my household has": 2,
        },
        "household_been_care_home_last_28_days": {
            "No, no one in my household has": 0,
            "Yes, I have": 1,
            "No I haven’t, but someone else in my household has": 2,
        },
        "physical_contact_under_18_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "physical_contact_18_to_69_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "physical_contact_over_70_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "social_distance_contact_under_18_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "social_distance_contact_18_to_69_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "social_distance_contact_over_70_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "face_covering_outside_of_home": {
            "No": 0,
            "Yes, at work/school only": 1,
            "Yes, in other situations only": 2,
            "Yes, usually both Work/school/other": 3,
            "My face is already covered": 4,
        },
        "face_covering_work_or_education": {
            "Never": 0,
            "Yes, sometimes": 1,
            "Yes, always": 2,
            "Not going to place of work or education": 3,
            "My face is already covered": 4,
            "My face is already covered for other reasons such as religious or cultural reasons": 5,
        },
        "face_covering_other_enclosed_places": {
            "Never": 0,
            "Yes, sometimes": 1,
            "Yes, always": 2,
            "Not going to other enclosed public spaces or using public transport": 3,
            "My face is already covered": 4,
            "My face is already covered for other reasons such as religious or cultural reasons": 5,
        },
        "other_covid_infection_test_results": {
            "Any tests negative, but none positive": 0,
            "One or more positive test(s)": 1,
            "Waiting for all results": 2,
            "All Tests failed": 9,
        },
        "other_antibody_test_results": {
            "Any tests negative, but none positive": 0,
            "One or more positive test(s)": 1,
            "Waiting for all results": 2,
            "All Tests failed": 9,
        },
        "other_antibody_test_location": {"In the NHS (e.g. GP, hospital)": 1, "Private lab": 2, "Home test": 3},
        "think_have_long_covid_symptom_reduced_ability": {
            "Not at all": 4,
            "Yes a little": 5,
            "Yes a lot": 6,
        },
        "participant_withdrawal_type": {
            "Withdrawn": 1,
            "Withdrawn_no_future_linkage_or_use of samples": 2,
            "Withdrawn_no_future_linkage": 3,
        },
        "country_barcode": {"England": 0, "Wales": 1, "NI": 2, "Scotland": 3},
        "not_attended_reason": {
            "At School": 1,
            "At Work": 2,
            "Doctor Appointment": 3,
            "Living Away - For Education": 4,
            "On Holiday": 5,
            "Other": 6,
            "Phone not answered": 7,
            "Planning to withdraw": 8,
        },
        "deferred": {"NA": 0, "Deferred": 1},
        "times_hour_or_longer_another_home_last_7_days": _times_in_last_7_day_categories,
        "times_hour_or_longer_another_person_your_home_last_7_days": _times_in_last_7_day_categories,
        "voucher_type_preference": {"email_address": 1, "Paper": 2},
        "participant_testing_group": {"Swab Only": 0, "Blood and Swab": 1, "Fingerprick and Swab": 2},
        "household_fingerprick_status": {
            "Accepted": 0,
            "Declined": 1,
            "Invited": 2,
            "At least one person consented": 3,
            "Not invited": 4,
            "No-one Consented": 5,
        },
        "able_to_take_blood": _yes_no_categories,
        "no_fingerprick_blood_taken_reason": {
            "Bruising or pain after first attempt": 0,
            "Couldn't get enough blood": 1,
            "No stock": 2,
            "Other": 3,
            "Participant felt unwell/fainted": 4,
            "Participant refused to give blood on this visit": 5,
            "Participant time constraints": 6,
            "Two attempts made": 7,
            "High risk assessment outcome": 8,
        },
        "no_venous_blood_taken_reason": {
            "Non-contact visit. Household self-isolating": 0,
            "Participant dehydrated": 1,
            "No stock": 2,
            "Other": 3,
            "Participant felt unwell/fainted": 4,
            "Participant refused": 5,
            "Participant time constraints": 6,
            "Poor venous access": 7,
            "Two attempts made": 8,
            "Bruising or pain after first attempt": 9,
        },
        "accepted_fingerprick_invite": _yes_no_categories,
        "cis_covid_vaccine_offered": _yes_no_categories,
        "regularly_lateral_flow_testing": _yes_no_categories,
        "household_visit_status": {
            "Completed": 1,
            "Dispatched": 2,
            "Household did not attend": 3,
            "Partially Completed": 4,
            "Withdrawn": 5,
        },
        "participant_survey_status": {"Active": 0, "Withdrawn": 1, "Completed": 2},
        "participant_withdrawal_reason": {
            "Bad experience with interviewer/survey": 1,
            "Moving location": 2,
            "No longer convenient": 3,
            "No longer wants to take part": 4,
            "Participant does not want to self swab": 5,
            "Swab/blood process too distressing": 6,
            "Too many visits": 7,
            "Household declined": 8,
            "Deceased": 9,
            "Do not reinstate": 10,
            "SWCAP": 11,
        },
        "cis_covid_vaccine_type": {
            "Don't know type": 1,
            "From a research study/trial": 2,
            "Moderna": 3,
            "Oxford/AstraZeneca": 4,
            "Pfizer/BioNTech": 5,
            "Other / specify": 6,
            "Janssen/Johnson&Johnson": 7,
            "Novavax": 8,
            "Sinovac": 9,
            "Sinovax": 10,
            "Valneva": 11,
            "Sinopharm": 12,
            "Sputnik": 13,
        },
        "cis_covid_vaccine_type_1": {
            "Don't know type": 1,
            "From a research study/trial": 2,
            "Moderna": 3,
            "Oxford/AstraZeneca": 4,
            "Pfizer/BioNTech": 5,
            "Other / specify": 6,
            "Janssen/Johnson&Johnson": 7,
            "Novavax": 8,
            "Sinovac": 9,
            "Sinovax": 10,
            "Valneva": 11,
            "Sinopharm": 12,
            "Sputnik": 13,
        },
        "cis_covid_vaccine_type_2": {
            "Don't know type": 1,
            "From a research study/trial": 2,
            "Moderna": 3,
            "Oxford/AstraZeneca": 4,
            "Pfizer/BioNTech": 5,
            "Other / specify": 6,
            "Janssen/Johnson&Johnson": 7,
            "Novavax": 8,
            "Sinovac": 9,
            "Sinovax": 10,
            "Valneva": 11,
            "Sinopharm": 12,
            "Sputnik": 13,
        },
        "cis_covid_vaccine_type_3": {
            "Don't know type": 1,
            "From a research study/trial": 2,
            "Moderna": 3,
            "Oxford/AstraZeneca": 4,
            "Pfizer/BioNTech": 5,
            "Other / specify": 6,
            "Janssen/Johnson&Johnson": 7,
            "Novavax": 8,
            "Sinovac": 9,
            "Sinovax": 10,
            "Valneva": 11,
            "Sinopharm": 12,
            "Sputnik": 13,
        },
        "cis_covid_vaccine_type_4": {
            "Don't know type": 1,
            "From a research study/trial": 2,
            "Moderna": 3,
            "Oxford/AstraZeneca": 4,
            "Pfizer/BioNTech": 5,
            "Other / specify": 6,
            "Janssen/Johnson&Johnson": 7,
            "Novavax": 8,
            "Sinovac": 9,
            "Sinovax": 10,
            "Valneva": 11,
            "Sinopharm": 12,
            "Sputnik": 13,
        },
        "cis_covid_vaccine_number_of_doses": {"1": 1, "2": 2, "3 or more": 3},
        "visit_date_type": {
            "actual_visit_date": 0,
            "latest_checkin_date": 1,
            "sample_taken_date": 2,
            "scheduled_date": 3,
        },
        "country_name_12": {"England": 0, "Wales": 1, "Northern Ireland": 2, "Scotland": 3},
        "local_authority_unity_authority_code": {
            "E06000001": 1,
            "E06000002": 2,
            "E06000003": 3,
            "E06000004": 4,
            "E06000005": 5,
            "E06000006": 6,
            "E06000007": 7,
            "E06000008": 8,
            "E06000009": 9,
            "E06000010": 10,
            "E06000011": 11,
            "E06000012": 12,
            "E06000013": 13,
            "E06000014": 14,
            "E06000015": 15,
            "E06000016": 16,
            "E06000017": 17,
            "E06000018": 18,
            "E06000019": 19,
            "E06000020": 20,
            "E06000021": 21,
            "E06000022": 22,
            "E06000023": 23,
            "E06000024": 24,
            "E06000025": 25,
            "E06000026": 26,
            "E06000027": 27,
            "E06000030": 28,
            "E06000031": 29,
            "E06000032": 30,
            "E06000033": 31,
            "E06000034": 32,
            "E06000035": 33,
            "E06000036": 34,
            "E06000037": 35,
            "E06000038": 36,
            "E06000039": 37,
            "E06000040": 38,
            "E06000041": 39,
            "E06000042": 40,
            "E06000043": 41,
            "E06000044": 42,
            "E06000045": 43,
            "E06000046": 44,
            "E06000047": 45,
            "E06000049": 46,
            "E06000050": 47,
            "E06000051": 48,
            "E06000052": 49,
            "E06000053": 50,
            "E06000054": 51,
            "E06000055": 52,
            "E06000056": 53,
            "E06000057": 54,
            "E06000058": 55,
            "E06000059": 56,
            "E06000060": 57,
            "E07000004": 58,
            "E07000005": 59,
            "E07000006": 60,
            "E07000007": 61,
            "E07000008": 62,
            "E07000009": 63,
            "E07000010": 64,
            "E07000011": 65,
            "E07000012": 66,
            "E07000026": 67,
            "E07000027": 68,
            "E07000028": 69,
            "E07000029": 70,
            "E07000030": 71,
            "E07000031": 72,
            "E07000032": 73,
            "E07000033": 74,
            "E07000034": 75,
            "E07000035": 76,
            "E07000036": 77,
            "E07000037": 78,
            "E07000038": 79,
            "E07000039": 80,
            "E07000040": 81,
            "E07000041": 82,
            "E07000042": 83,
            "E07000043": 84,
            "E07000044": 85,
            "E07000045": 86,
            "E07000046": 87,
            "E07000047": 88,
            "E07000061": 89,
            "E07000062": 90,
            "E07000063": 91,
            "E07000064": 92,
            "E07000065": 93,
            "E07000066": 94,
            "E07000067": 95,
            "E07000068": 96,
            "E07000069": 97,
            "E07000070": 98,
            "E07000071": 99,
            "E07000072": 100,
            "E07000073": 101,
            "E07000074": 102,
            "E07000075": 103,
            "E07000076": 104,
            "E07000077": 105,
            "E07000078": 106,
            "E07000079": 107,
            "E07000080": 108,
            "E07000081": 109,
            "E07000082": 110,
            "E07000083": 111,
            "E07000084": 112,
            "E07000085": 113,
            "E07000086": 114,
            "E07000087": 115,
            "E07000088": 116,
            "E07000089": 117,
            "E07000090": 118,
            "E07000091": 119,
            "E07000092": 120,
            "E07000093": 121,
            "E07000094": 122,
            "E07000095": 123,
            "E07000096": 124,
            "E07000098": 125,
            "E07000099": 126,
            "E07000102": 127,
            "E07000103": 128,
            "E07000105": 129,
            "E07000106": 130,
            "E07000107": 131,
            "E07000108": 132,
            "E07000109": 133,
            "E07000110": 134,
            "E07000111": 135,
            "E07000112": 136,
            "E07000113": 137,
            "E07000114": 138,
            "E07000115": 139,
            "E07000116": 140,
            "E07000117": 141,
            "E07000118": 142,
            "E07000119": 143,
            "E07000120": 144,
            "E07000121": 145,
            "E07000122": 146,
            "E07000123": 147,
            "E07000124": 148,
            "E07000125": 149,
            "E07000126": 150,
            "E07000127": 151,
            "E07000128": 152,
            "E07000129": 153,
            "E07000130": 154,
            "E07000131": 155,
            "E07000132": 156,
            "E07000133": 157,
            "E07000134": 158,
            "E07000135": 159,
            "E07000136": 160,
            "E07000137": 161,
            "E07000138": 162,
            "E07000139": 163,
            "E07000140": 164,
            "E07000141": 165,
            "E07000142": 166,
            "E07000143": 167,
            "E07000144": 168,
            "E07000145": 169,
            "E07000146": 170,
            "E07000147": 171,
            "E07000148": 172,
            "E07000149": 173,
            "E07000150": 174,
            "E07000151": 175,
            "E07000152": 176,
            "E07000153": 177,
            "E07000154": 178,
            "E07000155": 179,
            "E07000156": 180,
            "E07000163": 181,
            "E07000164": 182,
            "E07000165": 183,
            "E07000166": 184,
            "E07000167": 185,
            "E07000168": 186,
            "E07000169": 187,
            "E07000170": 188,
            "E07000171": 189,
            "E07000172": 190,
            "E07000173": 191,
            "E07000174": 192,
            "E07000175": 193,
            "E07000176": 194,
            "E07000177": 195,
            "E07000178": 196,
            "E07000179": 197,
            "E07000180": 198,
            "E07000181": 199,
            "E07000187": 200,
            "E07000188": 201,
            "E07000189": 202,
            "E07000192": 203,
            "E07000193": 204,
            "E07000194": 205,
            "E07000195": 206,
            "E07000196": 207,
            "E07000197": 208,
            "E07000198": 209,
            "E07000199": 210,
            "E07000200": 211,
            "E07000202": 212,
            "E07000203": 213,
            "E07000207": 214,
            "E07000208": 215,
            "E07000209": 216,
            "E07000210": 217,
            "E07000211": 218,
            "E07000212": 219,
            "E07000213": 220,
            "E07000214": 221,
            "E07000215": 222,
            "E07000216": 223,
            "E07000217": 224,
            "E07000218": 225,
            "E07000219": 226,
            "E07000220": 227,
            "E07000221": 228,
            "E07000222": 229,
            "E07000223": 230,
            "E07000224": 231,
            "E07000225": 232,
            "E07000226": 233,
            "E07000227": 234,
            "E07000228": 235,
            "E07000229": 236,
            "E07000234": 237,
            "E07000235": 238,
            "E07000236": 239,
            "E07000237": 240,
            "E07000238": 241,
            "E07000239": 242,
            "E07000240": 243,
            "E07000241": 244,
            "E07000242": 245,
            "E07000243": 246,
            "E07000244": 247,
            "E07000245": 248,
            "E07000246": 249,
            "E08000001": 250,
            "E08000002": 251,
            "E08000003": 252,
            "E08000004": 253,
            "E08000005": 254,
            "E08000006": 255,
            "E08000007": 256,
            "E08000008": 257,
            "E08000009": 258,
            "E08000010": 259,
            "E08000011": 260,
            "E08000012": 261,
            "E08000013": 262,
            "E08000014": 263,
            "E08000015": 264,
            "E08000016": 265,
            "E08000017": 266,
            "E08000018": 267,
            "E08000019": 268,
            "E08000021": 269,
            "E08000022": 270,
            "E08000023": 271,
            "E08000024": 272,
            "E08000025": 273,
            "E08000026": 274,
            "E08000027": 275,
            "E08000028": 276,
            "E08000029": 277,
            "E08000030": 278,
            "E08000031": 279,
            "E08000032": 280,
            "E08000033": 281,
            "E08000034": 282,
            "E08000035": 283,
            "E08000036": 284,
            "E08000037": 285,
            "E09000001": 286,
            "E09000002": 287,
            "E09000003": 288,
            "E09000004": 289,
            "E09000005": 290,
            "E09000006": 291,
            "E09000007": 292,
            "E09000008": 293,
            "E09000009": 294,
            "E09000010": 295,
            "E09000011": 296,
            "E09000012": 297,
            "E09000013": 298,
            "E09000014": 299,
            "E09000015": 300,
            "E09000016": 301,
            "E09000017": 302,
            "E09000018": 303,
            "E09000019": 304,
            "E09000020": 305,
            "E09000021": 306,
            "E09000022": 307,
            "E09000023": 308,
            "E09000024": 309,
            "E09000025": 310,
            "E09000026": 311,
            "E09000027": 312,
            "E09000028": 313,
            "E09000029": 314,
            "E09000030": 315,
            "E09000031": 316,
            "E09000032": 317,
            "E09000033": 318,
            "N09000001": 319,
            "N09000002": 320,
            "N09000003": 321,
            "N09000004": 322,
            "N09000005": 323,
            "N09000006": 324,
            "N09000007": 325,
            "N09000008": 326,
            "N09000009": 327,
            "N09000010": 328,
            "N09000011": 329,
            "S12000005": 330,
            "S12000006": 331,
            "S12000008": 332,
            "S12000010": 333,
            "S12000011": 334,
            "S12000013": 335,
            "S12000014": 336,
            "S12000017": 337,
            "S12000018": 338,
            "S12000019": 339,
            "S12000020": 340,
            "S12000021": 341,
            "S12000023": 342,
            "S12000026": 343,
            "S12000027": 344,
            "S12000028": 345,
            "S12000029": 346,
            "S12000030": 347,
            "S12000033": 348,
            "S12000034": 349,
            "S12000035": 350,
            "S12000036": 351,
            "S12000038": 352,
            "S12000039": 353,
            "S12000040": 354,
            "S12000041": 355,
            "S12000042": 356,
            "S12000045": 357,
            "S12000047": 358,
            "S12000048": 359,
            "S12000049": 360,
            "S12000050": 361,
            "W06000001": 362,
            "W06000002": 363,
            "W06000003": 364,
            "W06000004": 365,
            "W06000005": 366,
            "W06000006": 367,
            "W06000008": 368,
            "W06000009": 369,
            "W06000010": 370,
            "W06000011": 371,
            "W06000012": 372,
            "W06000013": 373,
            "W06000014": 374,
            "W06000015": 375,
            "W06000016": 376,
            "W06000018": 377,
            "W06000019": 378,
            "W06000020": 379,
            "W06000021": 380,
            "W06000022": 381,
            "W06000023": 382,
            "W06000024": 383,
        },
        "cis_area_code_20": {
            "J06000101": 1,
            "J06000102": 2,
            "J06000103": 3,
            "J06000104": 4,
            "J06000105": 5,
            "J06000106": 6,
            "J06000107": 7,
            "J06000108": 8,
            "J06000109": 9,
            "J06000110": 10,
            "J06000111": 11,
            "J06000112": 12,
            "J06000113": 13,
            "J06000114": 14,
            "J06000115": 15,
            "J06000116": 16,
            "J06000117": 17,
            "J06000118": 18,
            "J06000119": 19,
            "J06000120": 20,
            "J06000121": 21,
            "J06000122": 22,
            "J06000123": 23,
            "J06000124": 24,
            "J06000125": 25,
            "J06000126": 26,
            "J06000127": 27,
            "J06000128": 28,
            "J06000129": 29,
            "J06000130": 30,
            "J06000131": 31,
            "J06000132": 32,
            "J06000133": 33,
            "J06000134": 34,
            "J06000135": 35,
            "J06000136": 36,
            "J06000137": 37,
            "J06000138": 38,
            "J06000139": 39,
            "J06000140": 40,
            "J06000141": 41,
            "J06000142": 42,
            "J06000143": 43,
            "J06000144": 44,
            "J06000145": 45,
            "J06000146": 46,
            "J06000147": 47,
            "J06000148": 48,
            "J06000149": 49,
            "J06000150": 50,
            "J06000151": 51,
            "J06000152": 52,
            "J06000153": 53,
            "J06000154": 54,
            "J06000155": 55,
            "J06000156": 56,
            "J06000157": 57,
            "J06000158": 58,
            "J06000159": 59,
            "J06000160": 60,
            "J06000161": 61,
            "J06000162": 62,
            "J06000163": 63,
            "J06000164": 64,
            "J06000165": 65,
            "J06000166": 66,
            "J06000167": 67,
            "J06000168": 68,
            "J06000169": 69,
            "J06000170": 70,
            "J06000171": 71,
            "J06000172": 72,
            "J06000173": 73,
            "J06000174": 74,
            "J06000175": 75,
            "J06000176": 76,
            "J06000177": 77,
            "J06000178": 78,
            "J06000179": 79,
            "J06000180": 80,
            "J06000181": 81,
            "J06000182": 82,
            "J06000183": 83,
            "J06000184": 84,
            "J06000185": 85,
            "J06000186": 86,
            "J06000187": 87,
            "J06000188": 88,
            "J06000189": 89,
            "J06000190": 90,
            "J06000191": 91,
            "J06000192": 92,
            "J06000193": 93,
            "J06000194": 94,
            "J06000195": 95,
            "J06000196": 96,
            "J06000197": 97,
            "J06000198": 98,
            "J06000199": 99,
            "J06000200": 100,
            "J06000201": 101,
            "J06000202": 102,
            "J06000203": 103,
            "J06000204": 104,
            "J06000205": 105,
            "J06000206": 106,
            "J06000207": 107,
            "J06000208": 108,
            "J06000209": 109,
            "J06000210": 110,
            "J06000211": 111,
            "J06000212": 112,
            "J06000213": 113,
            "J06000214": 114,
            "J06000215": 115,
            "J06000216": 116,
            "J06000217": 117,
            "J06000218": 118,
            "J06000219": 119,
            "J06000220": 120,
            "J06000221": 121,
            "J06000222": 122,
            "J06000223": 123,
            "J06000224": 124,
            "J06000225": 125,
            "J06000226": 126,
            "J06000227": 127,
            "J06000228": 128,
            "J06000229": 129,
            "J06000230": 130,
            "J06000231": 131,
            "J06000232": 132,
            "J06000233": 133,
        },
        "people_in_household_count_group": {"1": 1, "2": 2, "3": 3, "4": 4, "5+": 5},
        "region_code": {
            "E12000001": 1,
            "E12000002": 2,
            "E12000003": 3,
            "E12000004": 4,
            "E12000005": 5,
            "E12000006": 6,
            "E12000007": 7,
            "E12000008": 8,
            "E12000009": 9,
            "N99999999": 10,
            "S99999999": 11,
            "W99999999": 12,
        },
    }
}
