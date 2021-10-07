iqvia_v2_time_map = {
    "dd/MM/yyyy": [
        "symptoms_last_7_days_onset_date",
        "last_covid_contact_date",
        "last_suspected_covid_contact_date",
        "think_had_covid_date",
        "pcr_test_first_positive_date",
        "pcr_test_last_negative_date",
        "antibody_test_first_positive_date",
        "antibody_test_last_negative_date",
        "vaccine_last_vaccination_date",
        "outside_uk_last_date",
    ],
    "dd/MM/yy HH:mm": ["date_of_birth"],
    "yyyy-MM-dd'T'HH:mm:ss.SSSS": ["visit_datetime", "samples_taken_datetime"],
}
swab_time_map = {"yyyy-MM-dd HH:mm:ss": ["pcr_datetime"]}

antibody_time_map = {
    "yyyy-MM-dd HH:mm:ss": ["blood_sample_collected_datetime"],
    "yyyy-MM-dd": ["blood_sample_arrayed_date", "antibody_test_result_recorded_date", "blood_sample_received_date"],
}
