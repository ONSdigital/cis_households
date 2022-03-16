survey_responses_v2_datetime_map = {
    "dd/MM/yyyy": [
        "symptoms_last_7_days_onset_date",
        "last_covid_contact_date",
        "last_suspected_covid_contact_date",
        "think_had_covid_date",
        "other_pcr_test_first_positive_date",
        "other_pcr_test_last_negative_date",
        "other_antibody_test_first_positive_date",
        "other_antibody_test_last_negative_date",
        "cis_covid_vaccine_date",
        "been_outside_uk_last_date",
        "improved_visit_date",
        "cis_covid_vaccine_date_1",
        "cis_covid_vaccine_date_2",
        "cis_covid_vaccine_date_3",
        "cis_covid_vaccine_date_4",
    ],
    "dd/MM/yyyy HH:mm": ["date_of_birth"],
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'": ["visit_datetime", "samples_taken_datetime"],
}

survey_responses_v1_datetime_map = {
    "dd/MM/yyyy": [
        "symptoms_last_7_days_onset_date",
        "last_covid_contact_date",
        "last_suspected_covid_contact_date",
        "think_had_covid_date",
        "other_pcr_test_first_positive_date",
        "other_pcr_test_last_negative_date",
        "cis_covid_vaccine_date",
        "been_outside_uk_last_date",
    ],
    "yyyy-MM-dd": [
        "date_of_birth",
        "other_antibody_test_first_positive_date",
        "other_antibody_test_last_negative_date",
    ],
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'": ["visit_datetime", "samples_taken_datetime"],
}

survey_responses_v0_datetime_map = {
    "dd/MM/yyyy": [
        "symptoms_last_7_days_onset_date",
        "last_covid_contact_date",
        "last_suspected_covid_contact_date",
        "think_had_covid_date",
        "date_of_birth",
    ],
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'": ["visit_datetime", "samples_taken_datetime"],
}

swab_datetime_map = {"yyyy-MM-dd HH:mm:ss": ["pcr_result_recorded_datetime"]}

blood_datetime_map = {
    "yyyy-MM-dd HH:mm:ss": ["blood_sample_collected_datetime"],
    "yyyy-MM-dd": ["blood_sample_arrayed_date", "antibody_test_result_recorded_date", "blood_sample_received_date"],
}
