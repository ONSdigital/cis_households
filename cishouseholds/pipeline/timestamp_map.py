csv_datetime_maps = {"imputed_value_lookup": {"yyyy-MM-dd": ["date_of_birth"]}}

lab_results_glasgow_datetime_map = {"yyyy-MM-dd HH:mm:ss 'UTC'": ["pcr_result_recorded_datetime"]}

survey_responses_v2_datetime_map = {
    "dd/MM/yyyy": [
        "think_have_covid_onset_date",
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
        "think_have_covid_onset_date",
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
        "think_have_covid_date",
        "last_covid_contact_date",
        "last_suspected_covid_contact_date",
        "think_had_covid_onset_date",
        "date_of_birth",
    ],
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'": ["visit_datetime", "samples_taken_datetime"],
}

cis_digital_datetime_map = {
    "yyyy-MM-dd'T'HH:mm:ss'Z'": [
        "participant_completion_window_start_datetime",
        "participant_completion_window_end_datetime",
        "swab_taken_datetime",
        "blood_taken_datetime",
        "visit_datetime",
        "samples_taken_datetime",
        "household_digital_enrolment_invited_datetime",
        "existing_participant_digital_opt_in_window_start_datetime",
        "existing_participant_digital_opt_in_window_end_datetime",
        "existing_participant_digital_opted_in_datetime",
        "household_digital_enrolment_datetime",
        "participant_digital_enrolment_datetime",
        "existing_participant_digital_opt_in_datetime",
        "digital_entry_pack_sent_datetime",
        "existing_participant_digital_opt_in_reminder_1_due_datetime",
        "existing_participant_digital_opt_in_reminder_1_sent_datetime",
        "existing_participant_digital_opt_in_reminder_2_due_datetime",
        "existing_participant_digital_opt_in_reminder_2_sent_datetime",
        "participant_completion_window_start_datetime",
        "participant_completion_window_end_datetime",
        "opted_out_of_next_window_datetime",
        "opted_out_of_blood_next_window_datetime",
        "sample_kit_dispatched_datetime",
        "sample_collection_kit_received_delivery_partner_datetime",
        "survey_last_modified_datetime",
        "survey_completed_datetime",
        "swab_sample_received_consolidation_point_datetime",
        "blood_sample_received_consolidation_point_datetime",
        "swab_sample_received_lab_datetime",
        "blood_sample_received_lab_datetime",
        "form_start_datetime",
    ],
    "yyyy-MM-dd": [
        "date_of_birth",
        "swab_return_date",
        "swab_return_future_date",
        "blood_return_date",
        "blood_return_future_date",
        "think_have_covid_onset_date",
        "cis_covid_vaccine_date_1",
        "cis_covid_vaccine_date_2",
        "cis_covid_vaccine_date_3",
        "cis_covid_vaccine_date_4",
        "cis_covid_vaccine_date_5",
        "cis_covid_vaccine_date_6",
        "been_outside_uk_last_return_date",
        "think_had_covid_onset_date",
        "other_covid_infection_test_first_positive_date",
        "other_covid_infection_test_last_negative_date",
        "other_antibody_test_first_positive_date",
        "other_antibody_test_last_negative_date",
        "last_covid_contact_date",
        "last_suspected_covid_contact_date",
        "cis_covid_vaccine_date",
        "think_have_covid_onset_date",
        "think_have_covid_symptom_onset_date",  # tempvar
        "other_covid_infection_test_first_positive_date",
        "other_covid_infection_test_positive_date",  # tempvar
        "other_covid_infection_test_last_negative_date",
        "other_covid_infection_test_negative_date",  # tempvar
        "other_antibody_test_first_positive_date",
        "other_antibody_test_positive_date",  # tempvar
        "other_antibody_test_last_negative_date",
        "other_antibody_test_negative_date",  # tempvar
        "sample_collection_courier_datetime",
    ],
}
