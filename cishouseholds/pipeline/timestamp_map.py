survey_responses_datetime_map = {
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
        "outside_uk_last_date",
    ],
    "dd/MM/yy HH:mm": ["date_of_birth"],
    "yyyy-MM-dd'T'HH:mm:ss.SSSS": ["visit_datetime", "samples_taken_datetime"],
}
swab_datetime_map = {"yyyy-MM-dd HH:mm:ss": ["pcr_datetime"]}

blood_datetime_map = {
    "yyyy-MM-dd HH:mm:ss": ["blood_sample_collected_datetime"],
    "yyyy-MM-dd": ["blood_sample_arrayed_date", "antibody_test_result_recorded_date", "blood_sample_received_date"],
}

historic_blood_datetime_map = {
    "yyyy-MM-dd HH:mm:ss": ["blood_sample_arrayed_date"],
    "yyyy-MM-dd": [
        "blood_sample_received_date",
        "antibody_test_result_recorded_date",
        "blood_sample_collected_datetime",
    ],
}
