survey_responses_v2_datetime_map = {
    "dd/MM/yyyy": [
        "think_have_covid_symptom_onset_date",
        "last_covid_contact_date",
        "last_suspected_covid_contact_date",
        "think_had_covid_onset_date",
        "other_covid_infection_test_first_positive_date",
        "other_covid_infection_test_last_negative_date",
        "other_antibody_test_first_positive_date",
        "other_antibody_test_last_negative_date",
        "cis_covid_vaccine_date",
        "been_outside_uk_last_return_date",
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
        "think_have_covid_symptom_onset_date",
        "last_covid_contact_date",
        "last_suspected_covid_contact_date",
        "think_had_covid_onset_date",
        "other_covid_infection_test_first_positive_date",
        "other_covid_infection_test_last_negative_date",
        "cis_covid_vaccine_date",
        "been_outside_uk_last_return_date",
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
        "think_have_covid_symptom_onset_date",
        "last_covid_contact_date",
        "last_suspected_covid_contact_date",
        "think_had_covid_onset_date",
        "date_of_birth",
    ],
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'": ["visit_datetime", "samples_taken_datetime"],
}

swab_datetime_map = {"yyyy-MM-dd HH:mm:ss": ["pcr_result_recorded_datetime"]}

blood_datetime_map = {
    "yyyy-MM-dd HH:mm:ss": ["blood_sample_collected_datetime"],
    "yyyy-MM-dd": ["blood_sample_arrayed_date", "antibody_test_result_recorded_date", "blood_sample_received_date"],
}

cis_digital_datetime_map = {
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'": [
        "participant_completion_window_start_datetime",
        "participant_completion_window_end_datetime",
        "opted_out_datetime",
        "opted_out_blood_datetime",
        "participant_enrolment_datetime",
        "sample_kit_dispatched_datetime",
        "sample_collection_courier_datetime",
        "sample_collection_kit_received_delivery_partner_datetime",
        "survey_last_modified_datetime",
        "survey_completed_datetime",
        "swab_sample_received_consolidation_point_datetime",
        "blood_sample_received_consolidation_point_datetime",
        "swab_sample_received_lab_datetime",
        "blood_sample_received_lab_datetime",
        "swab_taken_datetime",
        "blood_taken_datetime",
    ],
    "yyyy-MM-dd": [
        "swab_return_date",
        "swab_return_future_date",
        "blood_return_date",
        "blood_return_future_date",
        "think_have_covid_onset_date",
        "been_outside_uk_last_return_date",
        "think_had_covid_onset_date",
        "other_covid_infection_test_first_positive_date",
        "other_covid_infection_test_last_negative_date",
        "other_antibody_test_first_positive_date",
        "other_antibody_test_last_negative_date",
        "last_covid_contact_date",
        "cis_covid_vaccine_date",
        "date_of_birth",
    ],
}
