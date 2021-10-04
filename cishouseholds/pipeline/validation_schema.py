swab_allowed_pcr_results = ["Inconclusive", "Negative", "Positive", "Rejected"]

swab_validation_schema = {
    "swab_sample_barcode": {"type": "string", "regex": r"ONS\d{8}"},
    "pcr_result_classification": {"type": "string", "allowed": ["Negative", "Positive", "Void"]},
    "pcr_datetime": {"type": "string", "nullable": True},
    "pcr_lab_id": {"type": "string"},
    "pcr_method": {"type": "string"},
    "orf1ab_gene_pcr_target": {"type": "string", "allowed": ["ORF1ab"]},
    "orf1ab_gene_pcr_result_classification": {"type": "string", "allowed": swab_allowed_pcr_results},
    "orf1ab_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "n_gene_pcr_target": {"type": "string", "allowed": ["N gene"]},
    "n_gene_pcr_result_classification": {"type": "string", "allowed": swab_allowed_pcr_results},
    "n_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "s_gene_pcr_target": {"type": "string", "allowed": ["S gene"]},
    "s_gene_pcr_result_classification": {"type": "string", "allowed": swab_allowed_pcr_results},
    "s_gene_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
    "ms2_pcr_target": {"type": "string", "allowed": ["MS2"]},
    "ms2_pcr_result_classification": {"type": "string", "allowed": swab_allowed_pcr_results},
    "ms2_pcr_cq_value": {"type": "double", "nullable": True, "min": 0},
}


bloods_validation_schema = {
    "blood_sample_barcode": {"type": "string", "regex": r"ONS\d{8}"},
    "blood_sample_type": {"type": "string", "allowed": ["Venous", "Capillary"]},
    "antibody_test_plate_id": {"type": "string", "regex": r"(ON[BS]|MIX)_[0-9]{6}[C|V]S(-[0-9]+)"},
    "antibody_test_well_id": {"type": "string", "regex": r"[A-Z][0-9]{2}"},
    "antibody_test_result_classification": {"type": "string", "allowed": ["DETECTED", "NOT detected", "failed"]},
    "antibody_test_result_value": {"type": "double", "nullable": True, "min": 0},
    "antibody_test_bounded_result_value": {"type": "string"},
    "antibody_test_undiluted_result_value": {"type": "string"},
    "antibody_test_result_recorded_date": {"type": "string", "nullable": True},
    "blood_sample_arrayed_date": {"type": "string", "nullable": True},
    "blood_sample_received_date": {"type": "string", "nullable": True},
    "blood_sample_collected_datetime": {"type": "string", "nullable": True},
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

iqvia_v2_validation_schema = {
    "ons_household_id": {"type": "long", "min": 100000000000, "max": 999999999999},
    "visit_id": {"type": "string", "regex": r"DHV(F)?-\d{10}"},
    "visit_status": {
        "type": "string",
        "allowed": ["Completed", "Dispatched", "Household did not attend", "Partially Completed", "Withdrawn"],
    },
    "participant_visit_status": {
        "type": "string",
        "allowed": ["Cancelled", "Completed", "Patient did not attend", "Re-scheduled", "Scheduled"],
    },
    "participant_status": {"type": "string", "allowed": ["Active", "Completed", "Withdrawn"]},
    # Allowed options for below now different to what is in excel (more options now)
    "withdrawal_reason": {"type": "string", "nullable": True},
    # Will this have to be extended if can be in study >18 months?
    "type_of_visit": {"type": "string", "allowed": ["First Visit", "Follow-up Visit"]},
    # Allowed options for below now different to what is in excel (now can have at least month 19)
    "visit_order": {"type": "string"},
    "participant_testing_group": {"type": "string", "allowed": ["Blood and Swab", "Fingerprick and Swab", "Swab Only"]},
    "visit_datetime": {"type": "string", "nullable": True},
    # street, city, postcode are mandatory, but in the actual data there are a couple
    # records (3/30) that are null/empty strings
    "street": {"type": "string"},
    "city": {"type": "string"},
    "county": {"type": "string", "nullable": True},
    "postcode": {"type": "string"},
    "study_cohort": {"type": "string", "allowed": ["Blood and Swab", "Swab Only"]},
    "fingerprick_status_household": {
        "type": "string",
        "nullable": True,
        "allowed": ["Accepted", "At least one person consented", "Declined", "Invited", "Not invited"],
    },
    # Below variable removed in Protocol 9 - assume we can get rid of this validation (and dummy data) at some point
    "household_members_under_2_years": {"type": "string", "nullable": True, "allowed": ["Yes", "No"]},
    # Change this variable name in order to reflect what the age is measured in, then can also incorporate max?
    "infant_1_age": {"type": "float", "min": 0, "nullable": True},
    "infant_2_age": {"type": "float", "min": 0, "nullable": True},
    "infant_3_age": {"type": "float", "min": 0, "nullable": True},
    "infant_4_age": {"type": "float", "min": 0, "nullable": True},
    "infant_5_age": {"type": "float", "min": 0, "nullable": True},
    "infant_6_age": {"type": "float", "min": 0, "nullable": True},
    "infant_7_age": {"type": "float", "min": 0, "nullable": True},
    "infant_8_age": {"type": "float", "min": 0, "nullable": True},
    "household_members_over_2_years_and_not_present": {"type": "string", "nullable": True, "allowed": ["Yes", "No"]},
    # Why is the minimum here 0 rather than 2?
    "person_1_age": {"type": "float", "min": 0, "nullable": True},
    "person_2_age": {"type": "float", "min": 0, "nullable": True},
    "person_3_age": {"type": "float", "min": 0, "nullable": True},
    "person_4_age": {"type": "float", "min": 0, "nullable": True},
    "person_5_age": {"type": "float", "min": 0, "nullable": True},
    "person_6_age": {"type": "float", "min": 0, "nullable": True},
    "person_7_age": {"type": "float", "min": 0, "nullable": True},
    "person_8_age": {"type": "float", "min": 0, "nullable": True},
    # should I use max/min for limit the size of the sentence below?
    "person_1_not_consenting_age": {"type": "float", "min": 0, "nullable": True},
    # person_1_not_consenting_age is defined as float63, should it be an integer?
    "person_1_reason_for_not_consenting": {"type": "string", "nullable": True},
    # person_1 does not specify whether mandatory or not
    "person_2_not_consenting_age": {"type": "float", "min": 0, "nullable": True},
    "person_2_reason_for_not_consenting": {"type": "string", "nullable": True},
    "person_3_not_consenting_age": {"type": "float", "min": 0, "nullable": True},
    "person_3_reason_for_not_consenting": {"type": "string", "nullable": True},
    "person_4_not_consenting_age": {"type": "float", "min": 0, "nullable": True},
    "person_4_reason_for_not_consenting": {"type": "string", "nullable": True},
    "person_5_not_consenting_age": {"type": "float", "min": 0, "nullable": True},
    "person_5_reason_for_not_consenting": {"type": "string", "nullable": True},
    "person_6_not_consenting_age": {"type": "float", "min": 0, "nullable": True},
    "person_6_reason_for_not_consenting": {"type": "string", "nullable": True},
    "person_7_not_consenting_age": {"type": "float", "min": 0, "nullable": True},
    "person_7_reason_for_not_consenting": {"type": "string", "nullable": True},
    "person_8_not_consenting_age": {"type": "float", "min": 0, "nullable": True},
    "person_8_reason_for_not_consenting": {"type": "string", "nullable": True},
    "person_9_not_consenting_age": {"type": "float", "min": 0, "nullable": True},
    "person_9_reason_for_not_consenting": {"type": "string", "nullable": True},
    "participant_id": {"type": "string", "regex": r"DHR-\d{12}"},
    "title": {"type": "string", "allowed": ["Dr.", "Miss.", "Mr.", "Mrs.", "Ms.", "Prof."], "nullable": True},
    # No mandatory field: Title, Middle_Name, DoB, Email, Have_landline_number, Have_mobile_number,
    # Have_email_address, Prefer_receive_vouchers, Able_to_take_blood
    # double check the characters allowed for first/middle/last name. Make sure that the upper
    # limit applies in cerberus
    "first_name": {"type": "string"},  # why the example says [REDACTED] name?
    "middle_name": {"type": "string", "nullable": True},
    "last_name": {"type": "string"},
    "date_of_birth": {"type": "string", "nullable": True},
    # not sure about regex for emails, depends on how specific it is wanted
    "email": {"type": "string", "nullable": True},  # "regex": r"([a-zA-Z 0-9]|[@-_.]){8,49}"
    "have_landline_number": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "have_mobile_number": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "have_email_address": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "prefer_receive_vouchers": {"type": "string", "allowed": ["Email", "Paper(Post)"], "nullable": True},
    "confirm_receive_vouchers": {"type": "string", "allowed": ["false", "true"]},
    "no_email_address": {"type": "integer", "min": 0, "max": 1},  # amend dummy data to reflect 0/1 options
    "able_to_take_blood": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "no_blood_reason_fingerprick": {
        "type": "string",
        "allowed": [
            "Bruising or pain after first attempt",
            "Couldn't get enough blood",
            "No stock",
            "Other",
            "Participant felt unwell/fainted",
            "Participant refused to give blood on this visit",
            "Participant time constraints",
            "Two attempts made",
        ],
        "nullable": True,
    },
    "no_blood_reason_venous": {"type": "string", "nullable": True},
    "blood_sample_barcode": {"type": "string", "regex": r"ON[SWCN]\d{8}"},
    "swab_sample_barcode": {"type": "string", "regex": r"ON[SWCN]\d{8}"},
    "samples_taken_datetime": {"type": "string", "nullable": True},
    "sex": {"type": "string", "allowed": ["Male", "Female"], "nullable": True},
    "gender": {"type": "string", "allowed": ["Male", "Female", "Prefer not to say"], "nullable": True},
    "ethnic_group": {
        "type": "string",
        "allowed": [
            "Asian or Asian British",
            "Black or African or Caribbean or Black British",
            "Mixed/Multiple Ethnic Groups",
            "Other Ethnic Group",
            "White",
        ],
    },
    "ethnicity": {
        "type": "string",
        "allowed": [
            "Any other Asian background",
            "Any other Black background",
            "Any other Mixed background",
            "Any other ethnic group",
            "Any other white background",
            "Asian or Asian British-Bangladeshi",
            "Asian or Asian British-Chinese",
            "Asian or Asian British-Indian",
            "Asian or Asian British-Pakistani",
            "Black,Caribbean,African-African",
            "Black,Caribbean,Afro-Caribbean",
            "Mixed-White & Asian",
            "Mixed-White & Black African",
            "Mixed-White & Black Caribbean",
            "Other ethnic group-Arab",
            "White-British",
            "White-Gypsy or Irish Traveller",
            "White-Irish",
        ],
    },
    "ethnicity_other": {"type": "string", "nullable": True},
    "consent_1_visit": {"type": "string", "allowed": ["No", "Yes"]},
    "consent_5_visits": {"type": "string", "allowed": ["No", "Yes"]},
    "consent_april_22": {"type": "string", "allowed": ["No", "Yes"]},
    "consent_16_visits": {"type": "string", "allowed": ["No", "Yes"]},
    "consent_blood_test": {"type": "string", "allowed": ["No", "Yes"]},
    "consent_finger_prick_a1_a3": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "consent_extend_study_under_16_b1_b3": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "consent_contact_extra_research": {"type": "string", "allowed": ["No", "Yes"]},
    "consent_contact_extra_researchyn": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "consent_use_of_surplus_blood_samples": {"type": "string", "allowed": ["No", "Yes"]},
    "consent_use_of_surplus_blood_samplesyn": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "approached_for_blood_samples?": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "consent_blood_samples_if_positive": {"type": "string", "allowed": ["False", "True"]},
    "consent_blood_samples_if_positiveyn": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "consent_fingerprick_blood_samples": {"type": "string", "allowed": ["False", "True"]},
    "accepted_invite_fingerprick": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "reconsented_blood": {"type": "string", "allowed": ["False", "True"]},
    "work_main_job_title": {"type": "string"},
    "work_main_job_role": {"type": "string"},
    "work_sectors": {
        "type": "string",
        "allowed": [
            "Armed forces",
            "Art or entertainment or recreation",
            "Arts or Entertainment or Recreation",
            "Arts or entertainment or recreation",
            "Civil Service or Local Government",
            "Financial Services (incl. insurance)",
            "Financial services (incl. insurance)",
            "Food Production and agriculture (incl. farming)",
            "Food production and agriculture (incl. farming)",
            "Health care",
            "Hospitality (e.g. hotel or restaurant or cafe)",
            "Information technology and communication",
            "Manufacturing or construction",
            "Other employment sector (specify)",
            "Other occupation sector",
            "Other occupation sector (specify)",
            "Personal Services (e.g. hairdressers or tattooists)",
            "Retail Sector (incl. wholesale)",
            "Retail sector (incl. wholesale)",
            "Social Care|Social care",
            "Teaching and education",
            "Transport (incl. storage and logistic)",
            "Transport (incl. storage and logistics)",
            "Transport (incl. storage or logistic)",
        ],
    },
    "work_sectors_other": {"type": "string", "nullable": True},
    "work_nursing_or_residential_care_home": {
        "type": "string",
        "nullable": True,
        "allowed": ["No", "Participant Would Not/Could Not Answer", "Yes"],
    },
    "work_healthcare": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Other Healthcare (e.g. mental health)",
            "Other healthcare (e.g. mental health)",
            "Participant Would Not/Could Not Answer",
            "Primary Care (e.g. GP or dentist)",
            "Primary care (e.g. GP or dentist)",
            "Secondary Care (e.g. hospital)",
            "Secondary care (e.g. hospital)",
            "Secondary care (e.g. hospital.)",
        ],
    },
    "work_direct_contact_persons": {
        "type": "string",
        "nullable": True,
        "allowed": ["No", "Participant Would Not/Could Not Answer", "Yes"],
    },
    "illness_lasting_over_12_months": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "illness_reduces_activity_or_ability": {
        "type": "string",
        "nullable": True,
        "allowed": ["Not at all", "Participant Would Not/Could Not Answer", "Yes a little", "Yes a lot"],
    },
    "have_ever_smoked_regularly": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "smokes_or_vapes_description": {"type": "string", "nullable": True},
    "smokes_or_vapes": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "smoke_cigarettes": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "smokes_cigar": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "smokes_pipe": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "smokes_vape_e_cigarettes": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "smokes_hookah_shisha_pipes": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "work_status": {"type": "string", "nullable": True},
    "work_in_additional_paid_employment": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "work_main_job_changed": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "work_location": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Both (work from home and work somewhere else)",
            "Both (working from home and working somewhere else)",
            "From home (in the same grounds or building as your home)",
            "Somewhere else (not at your home)",
            "Somewhere else (not your home)",
            "Work from home (in the same grounds or building as your home)",
            "Work somewhere else (not your home)",
            "Working from home (in the same grounds or building as your home)",
        ],
    },
    "work_not_from_home_days_per_week": {
        "type": "string",
        "nullable": True,
        "allowed": ["0", "1", "2", "3", "4", "5", "6", "7", "Participant Would Not/Could Not Answer", "up to 1"],
    },
    "transport_to_work_or_school": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Bicycle",
            "Bus",
            "Bus or Minibus or Coach",
            "Bus or minibus or coach",
            "Car or Van",
            "Car or van",
            "Motorbike or Scooter or Moped",
            "Motorbike or scooter or moped",
            "On Foot",
            "On foot",
            "Other Method",
            "Other method",
            "Participant Would Not/Could Not Answer",
            "Taxi/Minicab",
            "Taxi/minicab",
            "Train",
            "Underground or Metro or Light Rail or Tram",
        ],
    },
    "ability_to_socially_distance_at_work_or_school": {"type": "string", "nullable": True},
    "had_symptoms_last_7_days": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "which_symptoms_last_7_days": {"type": "string", "nullable": True},
    "symptoms_last_7_days_onset_date": {"type": "string", "nullable": True},
    "symptoms_last_7_days_fever": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "symptoms_last_7_days_muscle_ache_myalgia": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "symptoms_last_7_days_fatigue_weakness": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "symptoms_last_7_days_sore_throat": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "symptoms_last_7_days_cough": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "symptoms_last_7_days_shortness_of_breath": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "symptoms_last_7_days_headache": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "symptoms_last_7_days_nausea_vomiting": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "symptoms_last_7_days_abdominal_pain": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "symptoms_last_7_days_diarrhoea": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "symptoms_last_7_days_loss_of_taste": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "symptoms_last_7_days_loss_of_smell": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "is_self_isolating": {"type": "string", "nullable": True},
    "think_have_covid_symptoms": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "contact_known_positive_covid_last_28_days": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "last_covid_contact_date": {"type": "string", "nullable": True},
    "last_covid_contact_location": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Living in your own home",
            "Outside your home",
        ],
    },
    "contact_suspect_positive_covid_last_28_days": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "last_suspected_covid_contact_date": {"type": "string", "nullable": True},
    "last_suspected_covid_contact_location": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Living in your own home",
            "Outside your home",
        ],
    },
    "hospital_last_28_days": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "hospital_last_28_days_other_household_member": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "care_home_last_28_days": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "care_home_last_28_days_other_household_member": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "hours_a_day_with_someone_else_at_home": {"type": "integer", "nullable": True, "min": 0, "max": 24},
    "physical_contact_under_18_years": {
        "type": "string",
        "nullable": True,
        "allowed": ["0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer"],
    },
    "physical_contact_18_to_69_years": {
        "type": "string",
        "nullable": True,
        "allowed": ["0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer"],
    },
    "physical_contact_over_70_years": {
        "type": "string",
        "nullable": True,
        "allowed": ["0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer"],
    },
    "social_distance_contact_under_18_years": {
        "type": "string",
        "nullable": True,
        "allowed": ["0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer"],
    },
    "social_distance_contact_18_to_69_years": {
        "type": "string",
        "nullable": True,
        "allowed": ["0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer"],
    },
    "social_distance_contact_over_70_years": {
        "type": "string",
        "nullable": True,
        "allowed": ["0", "1-5", "11-20", "21 or more", "6-10", "Participant Would Not/Could Not Answer"],
    },
    "times_hour_or_longer_another_home_last_7_days": {
        "type": "string",
        "nullable": True,
        "allowed": ["1", "2", "3", "4", "5", "6", "7 times or more", "None", "Participant Would Not/Could Not Answer"],
    },
    "times_hour_or_longer_another_person_your_home_last_7_days": {
        "type": "string",
        "nullable": True,
        "allowed": ["1", "2", "3", "4", "5", "6", "7 times or more", "None", "Participant Would Not/Could Not Answer"],
    },
    "times_outside_shopping_or_socialising_last_7_days": {
        "type": "string",
        "nullable": True,
        "allowed": ["1", "2", "3", "4", "5", "6", "7 times or more", "None", "Participant Would Not/Could Not Answer"],
    },
    "times_shopping_last_7_days": {
        "type": "string",
        "nullable": True,
        "allowed": ["1", "2", "3", "4", "5", "6", "7 times or more", "None", "Participant Would Not/Could Not Answer"],
    },
    "times_socialise_last_7_days": {
        "type": "string",
        "nullable": True,
        "allowed": ["1", "2", "3", "4", "5", "6", "7 times or more", "None", "Participant Would Not/Could Not Answer"],
    },
    "is_regularly_lateral_flow_testing": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Yes",
            "No",
        ],
    },
    "face_covering_outside_of_home": {"type": "string", "nullable": True},
    "face_covering_work": {"type": "string", "nullable": True},
    "face_covering_other_enclosed_places": {"type": "string", "nullable": True},
    "think_had_covid": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "think_had_covid_any_symptoms": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "think_had_covid_which_symptoms": {"type": "string", "nullable": True},
    "symptoms_since_last_visit_fever": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "symptoms_since_last_visit_muscle_ache_myalgia": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "symptoms_since_last_visit_fatigue_weakness": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "symptoms_since_last_visit_sore_throat": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "symptoms_since_last_visit_cough": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "symptoms_since_last_visit_shortness_of_breath": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "symptoms_since_last_visit_headache": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "symptoms_since_last_visit_nausea_vomiting": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "symptoms_since_last_visit_abdominal_pain": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "symptoms_since_last_visit_diarrhoea": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "symptoms_since_last_visit_loss_of_taste": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "symptoms_since_last_visit_loss_of_smell": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "think_had_covid_date": {"type": "string", "nullable": True},
    "think_had_covid_contacted_nhs": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "think_had_covid_admitted_to_hospital": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "pcr_test_since_last_visit": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "pcr_test_since_last_visit_results": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "pcr_test_first_positive_date": {"type": "string", "nullable": True},
    "pcr_test_last_negative_date": {"type": "string", "nullable": True},
    "antibody_test_since_last_visit": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "antibody_test_since_last_visit_results": {"type": "string", "allowed": ["No", "Yes"], "nullable": True},
    "antibody_test_since_last_visit_location": {
        "type": "string",
        "allowed": [
            "Home Test",
            "In the NHS (e.g. GP or hospital)",
            "Participant Would Not/Could Not Answer",
            "Private Lab",
        ],
        "nullable": True,
    },
    "antibody_test_first_positive_date": {"type": "string", "nullable": True},
    "antibody_test_last_negative_date": {"type": "string", "nullable": True},
    "have_long_covid_symptoms": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_reduce_activities": {
        "type": "string",
        "nullable": True,
        "allowed": ["Not at all", "Yes a little", "Yes a lot"],
    },
    "long_covid_symptoms": {"type": "string", "nullable": True},
    "long_covid_fever": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_weakness_tiredness": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_diarrhoea": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_loss_of_smell": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_shortness_of_breath": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_vertigo_dizziness": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_trouble_sleeping": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_headache": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_nausea_vomiting": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_loss_of_appetite": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_sore_throat": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_chest_pain": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_worry_anxiety": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_memory_loss_or_confusion": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_muscle_ache": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_abdominal_pain": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_loss_of_taste": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_cough": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_palpitations": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_low_mood": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "long_covid_difficulty_concentrating": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "vaccine_offered_since_last_visit": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "vaccine_received_since_last_visit": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    # This may change as other vaccines are considered "acceptable"
    "vaccine_type_since_last_visit": {
        "type": "string",
        "nullable": True,
        "allowed": [
            "Don't know type",
            "From a research study/trial",
            "Janssen/Johnson&Johnson",
            "Moderna",
            "Novavax",
            "Other / specify",
            "Oxford/AstraZeneca",
            "Pfizer/BioNTech",
            "Sinopharm",
            "Sinovac",
            "Sinovax",
            "Sputnik",
            "Valneva",
        ],
    },
    "vaccine_type_other_since_last_visit": {"type": "string", "nullable": True},
    "vaccine_number_of_doses": {"type": "string", "nullable": True, "allowed": ["1", "2", "3 or more"]},
    "vaccine_last_vaccination_date": {"type": "string", "nullable": True},
    "outside_uk_since_april_2020": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
    "outside_uk_last_country": {"type": "string", "nullable": True},
    "outside_uk_last_date": {"type": "string", "nullable": True},
    "outside_uk_since_last_visit": {"type": "string", "nullable": True, "allowed": ["No", "Yes"]},
}
