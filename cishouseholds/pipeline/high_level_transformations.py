# flake8: noqa
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_age_at_date
from cishouseholds.derive import assign_column_from_mapped_list_key
from cishouseholds.derive import assign_column_given_proportion
from cishouseholds.derive import assign_column_regex_match
from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import assign_consent_code
from cishouseholds.derive import assign_date_difference
from cishouseholds.derive import assign_date_from_filename
from cishouseholds.derive import assign_datetime_from_coalesced_columns_and_log_source
from cishouseholds.derive import assign_ethnicity_white
from cishouseholds.derive import assign_ever_had_long_term_health_condition_or_disabled
from cishouseholds.derive import assign_fake_id
from cishouseholds.derive import assign_first_visit
from cishouseholds.derive import assign_grouped_variable_from_days_since
from cishouseholds.derive import assign_household_participant_count
from cishouseholds.derive import assign_household_under_2_count
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import assign_last_visit
from cishouseholds.derive import assign_named_buckets
from cishouseholds.derive import assign_raw_copies
from cishouseholds.derive import assign_regex_match_result
from cishouseholds.derive import assign_school_year_september_start
from cishouseholds.derive import assign_substring
from cishouseholds.derive import assign_taken_column
from cishouseholds.derive import assign_test_target
from cishouseholds.derive import assign_true_if_any
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.derive import assign_visit_order
from cishouseholds.derive import assign_work_health_care
from cishouseholds.derive import assign_work_patient_facing_now
from cishouseholds.derive import assign_work_person_facing_now
from cishouseholds.derive import assign_work_social_column
from cishouseholds.derive import assign_work_status_group
from cishouseholds.derive import concat_fields_if_true
from cishouseholds.derive import contact_known_or_suspected_covid_type
from cishouseholds.derive import count_value_occurrences_in_column_subset_row_wise
from cishouseholds.derive import derive_cq_pattern
from cishouseholds.derive import derive_had_symptom_last_7days_from_digital
from cishouseholds.derive import derive_household_been_columns
from cishouseholds.derive import flag_records_for_childcare_v1_rules
from cishouseholds.derive import flag_records_for_childcare_v2_b_rules
from cishouseholds.derive import flag_records_for_college_v0_rules
from cishouseholds.derive import flag_records_for_college_v2_rules
from cishouseholds.derive import flag_records_for_furlough_rules_v0
from cishouseholds.derive import flag_records_for_furlough_rules_v1_a
from cishouseholds.derive import flag_records_for_furlough_rules_v1_b
from cishouseholds.derive import flag_records_for_furlough_rules_v2_a
from cishouseholds.derive import flag_records_for_furlough_rules_v2_b
from cishouseholds.derive import flag_records_for_not_working_rules_v0
from cishouseholds.derive import flag_records_for_not_working_rules_v1_a
from cishouseholds.derive import flag_records_for_not_working_rules_v1_b
from cishouseholds.derive import flag_records_for_not_working_rules_v2_a
from cishouseholds.derive import flag_records_for_not_working_rules_v2_b
from cishouseholds.derive import flag_records_for_retired_rules
from cishouseholds.derive import flag_records_for_school_v2_rules
from cishouseholds.derive import flag_records_for_self_employed_rules_v1_a
from cishouseholds.derive import flag_records_for_self_employed_rules_v1_b
from cishouseholds.derive import flag_records_for_self_employed_rules_v2_a
from cishouseholds.derive import flag_records_for_self_employed_rules_v2_b
from cishouseholds.derive import flag_records_for_student_v0_rules
from cishouseholds.derive import flag_records_for_student_v1_rules
from cishouseholds.derive import flag_records_for_uni_v0_rules
from cishouseholds.derive import flag_records_for_uni_v2_rules
from cishouseholds.derive import flag_records_for_work_from_home_rules
from cishouseholds.derive import flag_records_for_work_location_null
from cishouseholds.derive import get_keys_by_value
from cishouseholds.derive import map_options_to_bool_columns
from cishouseholds.derive import mean_across_columns
from cishouseholds.derive import regex_match_result
from cishouseholds.derive import translate_column_regex_replace
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import assign_from_map
from cishouseholds.edit import clean_barcode
from cishouseholds.edit import clean_barcode_simple
from cishouseholds.edit import clean_job_description_string
from cishouseholds.edit import clean_postcode
from cishouseholds.edit import clean_within_range
from cishouseholds.edit import convert_null_if_not_in_list
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.edit import format_string_upper_and_clean
from cishouseholds.edit import map_column_values_to_null
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_column_if_ref_in_list
from cishouseholds.edit import update_column_in_time_window
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_face_covering_outside_of_home
from cishouseholds.edit import update_person_count_from_ages
from cishouseholds.edit import update_strings_to_sentence_case
from cishouseholds.edit import update_think_have_covid_symptom_any
from cishouseholds.edit import update_to_value_if_any_not_null
from cishouseholds.edit import update_work_facing_now_column
from cishouseholds.expressions import any_column_null
from cishouseholds.expressions import sum_within_row
from cishouseholds.impute import fill_backwards_overriding_not_nulls
from cishouseholds.impute import fill_backwards_work_status_v2
from cishouseholds.impute import fill_forward_from_last_change
from cishouseholds.impute import fill_forward_from_last_change_marked_subset
from cishouseholds.impute import fill_forward_only_to_nulls
from cishouseholds.impute import fill_forward_only_to_nulls_in_dataset_based_on_column
from cishouseholds.impute import impute_and_flag
from cishouseholds.impute import impute_by_distribution
from cishouseholds.impute import impute_by_k_nearest_neighbours
from cishouseholds.impute import impute_by_mode
from cishouseholds.impute import impute_by_ordered_fill_forward
from cishouseholds.impute import impute_date_by_k_nearest_neighbours
from cishouseholds.impute import impute_latest_date_flag
from cishouseholds.impute import impute_outside_uk_columns
from cishouseholds.impute import impute_visit_datetime
from cishouseholds.impute import merge_previous_imputed_values
from cishouseholds.pipeline.mapping import _welsh_ability_to_socially_distance_at_work_or_education_categories
from cishouseholds.pipeline.mapping import _welsh_blood_kit_missing_categories
from cishouseholds.pipeline.mapping import _welsh_blood_not_taken_reason_categories
from cishouseholds.pipeline.mapping import _welsh_blood_sample_not_taken_categories
from cishouseholds.pipeline.mapping import _welsh_cis_covid_vaccine_number_of_doses_categories
from cishouseholds.pipeline.mapping import _welsh_contact_type_by_age_group_categories
from cishouseholds.pipeline.mapping import _welsh_currently_smokes_or_vapes_description_categories
from cishouseholds.pipeline.mapping import _welsh_face_covering_categories
from cishouseholds.pipeline.mapping import _welsh_live_with_categories
from cishouseholds.pipeline.mapping import _welsh_lot_little_not_categories
from cishouseholds.pipeline.mapping import _welsh_number_of_types_categories
from cishouseholds.pipeline.mapping import _welsh_other_covid_infection_test_result_categories
from cishouseholds.pipeline.mapping import _welsh_self_isolating_reason_detailed_categories
from cishouseholds.pipeline.mapping import _welsh_swab_kit_missing_categories
from cishouseholds.pipeline.mapping import _welsh_swab_sample_not_taken_categories
from cishouseholds.pipeline.mapping import _welsh_transport_to_work_education_categories
from cishouseholds.pipeline.mapping import _welsh_vaccination_type_categories
from cishouseholds.pipeline.mapping import _welsh_work_location_categories
from cishouseholds.pipeline.mapping import _welsh_work_sector_categories
from cishouseholds.pipeline.mapping import _welsh_work_status_digital_categories
from cishouseholds.pipeline.mapping import _welsh_work_status_education_categories
from cishouseholds.pipeline.mapping import _welsh_work_status_employment_categories
from cishouseholds.pipeline.mapping import _welsh_work_status_unemployment_categories
from cishouseholds.pipeline.mapping import _welsh_yes_no_categories
from cishouseholds.pipeline.mapping import column_name_maps
from cishouseholds.pipeline.regex_patterns import at_school_pattern
from cishouseholds.pipeline.regex_patterns import at_university_pattern
from cishouseholds.pipeline.regex_patterns import childcare_pattern
from cishouseholds.pipeline.regex_patterns import furloughed_pattern
from cishouseholds.pipeline.regex_patterns import in_college_or_further_education_pattern
from cishouseholds.pipeline.regex_patterns import not_working_pattern
from cishouseholds.pipeline.regex_patterns import retired_regex_pattern
from cishouseholds.pipeline.regex_patterns import self_employed_regex
from cishouseholds.pipeline.regex_patterns import work_from_home_pattern
from cishouseholds.pipeline.regex_testing import healthcare_pattern
from cishouseholds.pipeline.regex_testing import patient_facing_pattern
from cishouseholds.pipeline.regex_testing import social_care_pattern
from cishouseholds.pipeline.timestamp_map import cis_digital_datetime_map
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate_class import SparkValidate

# from cishouseholds.pipeline.regex_patterns import healthcare_bin_pattern


def transform_cis_soc_data(df: DataFrame, join_on_columns: List[str]) -> DataFrame:
    """
    transform and process cis soc data
    """
    df = df.withColumn(
        "standard_occupational_classification_code",
        F.when(F.substring(F.col("standard_occupational_classification_code"), 1, 2) == "un", "uncodeable").otherwise(
            F.col("standard_occupational_classification_code")
        ),
    )

    # remove nulls and deduplicate on all columns
    df = df.filter(F.col("work_main_job_title").isNotNull() & F.col("work_main_job_role").isNotNull()).distinct()

    df = df.withColumn(
        "LENGTH",
        F.length(
            F.when(
                F.col("standard_occupational_classification_code") != "uncodeable",
                F.col("standard_occupational_classification_code"),
            )
        ),
    ).orderBy(F.desc("LENGTH"))

    window = Window.partitionBy(*join_on_columns)
    df = df.withColumn("DROP", F.col("LENGTH") != F.max("LENGTH").over(window))
    df = df.filter((F.col("standard_occupational_classification_code") != "uncodeable") & (~F.col("DROP")))
    return df.drop("DROP", "LENGTH")


def transform_survey_responses_version_0_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 0 survey deltas.
    """
    df = assign_taken_column(df=df, column_name_to_assign="swab_taken", reference_column="swab_sample_barcode")
    df = assign_taken_column(df=df, column_name_to_assign="blood_taken", reference_column="blood_sample_barcode")

    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 0)
    df = df.withColumn("sex", F.coalesce(F.col("sex"), F.col("gender"))).drop("gender")

    df = map_column_values_to_null(
        df=df,
        value="Participant Would Not/Could Not Answer",
        column_list=[
            "ethnicity",
            "work_status_v0",
            "work_location",
            "survey_response_type",
            "participant_withdrawal_reason",
            "work_not_from_home_days_per_week",
        ],
    )

    # Create before editing to v1 version below
    df = df.withColumn("work_health_care_area", F.col("work_health_care_patient_facing"))

    column_editing_map = {
        "work_health_care_area": {
            "Yes, primary care, patient-facing": "Yes, in primary care, e.g. GP, dentist",
            "Yes, secondary care, patient-facing": "Yes, in secondary care, e.g. hospital",
            "Yes, other healthcare, patient-facing": "Yes, in other healthcare settings, e.g. mental health",
            "Yes, primary care, non-patient-facing": "Yes, in primary care, e.g. GP, dentist",
            "Yes, secondary care, non-patient-facing": "Yes, in secondary care, e.g. hospital",
            "Yes, other healthcare, non-patient-facing": "Yes, in other healthcare settings, e.g. mental health",
        },
        "work_location": {
            "Both (working from home and working outside of your home)": "Both (from home and somewhere else)",
            "Working From Home": "Working from home",
            "Working Outside of your Home": "Working somewhere else (not your home)",
            "Not applicable": "Not applicable, not currently working",
        },
        "last_covid_contact_type": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
        "last_suspected_covid_contact_type": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
        "other_covid_infection_test_results": {
            "Positive": "One or more positive test(s)",
            "Negative": "Any tests negative, but none positive",
        },
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)

    df = clean_barcode(df=df, barcode_column="swab_sample_barcode", edited_column="swab_sample_barcode_edited_flag")
    df = clean_barcode(df=df, barcode_column="blood_sample_barcode", edited_column="blood_sample_barcode_edited_flag")
    df = df.drop(
        "cis_covid_vaccine_date",
        "cis_covid_vaccine_number_of_doses",
        "cis_covid_vaccine_type",
        "cis_covid_vaccine_type_other",
        "cis_covid_vaccine_received",
    )
    return df


def clean_survey_responses_version_1(df: DataFrame) -> DataFrame:
    df = map_column_values_to_null(
        df=df,
        value="Participant Would Not/Could Not Answer",
        column_list=[
            "ethnicity",
            "work_sector",
            "work_health_care_area",
            "work_status_v1",
            "work_location",
            "work_direct_contact_patients_or_clients",
            "survey_response_type",
            "self_isolating_reason",
            "illness_reduces_activity_or_ability",
            "ability_to_socially_distance_at_work_or_education",
            "transport_to_work_or_education",
            "face_covering_outside_of_home",
            "other_antibody_test_location",
            "participant_withdrawal_reason",
            "work_not_from_home_days_per_week",
        ],
    )

    df = df.withColumn("work_main_job_changed", F.lit(None).cast("string"))
    fill_forward_columns = [
        "work_main_job_title",
        "work_main_job_role",
        "work_sector",
        "work_sector_other",
        "work_health_care_area",
        "work_nursing_or_residential_care_home",
        "work_direct_contact_patients_or_clients",
    ]
    df = update_to_value_if_any_not_null(
        df=df,
        column_name_to_assign="work_main_job_changed",
        value_to_assign="Yes",
        column_list=fill_forward_columns,
    )
    df = df.drop(
        "cis_covid_vaccine_date",
        "cis_covid_vaccine_number_of_doses",
        "cis_covid_vaccine_type",
        "cis_covid_vaccine_type_other",
        "cis_covid_vaccine_received",
    )
    return df


def transform_survey_responses_version_1_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 1 survey deltas.
    """
    df = assign_taken_column(df=df, column_name_to_assign="swab_taken", reference_column="swab_sample_barcode")
    df = assign_taken_column(df=df, column_name_to_assign="blood_taken", reference_column="blood_sample_barcode")

    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 1)

    df = df.withColumn("work_status_v0", F.col("work_status_v1"))
    df = df.withColumn("work_status_v2", F.col("work_status_v1"))

    been_value_map = {"No, someone else in my household has": "No I haven’t, but someone else in my household has"}
    column_editing_map = {
        "work_status_v0": {
            "Employed and currently working": "Employed",  # noqa: E501
            "Employed and currently not working": "Furloughed (temporarily not working)",  # noqa: E501
            "Self-employed and currently not working": "Furloughed (temporarily not working)",  # noqa: E501
            "Retired": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Looking for paid work and able to start": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Not working and not looking for work": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Child under 5y not attending child care": "Student",  # noqa: E501
            "Child under 5y attending child care": "Student",  # noqa: E501
            "5y and older in full-time education": "Student",  # noqa: E501
            "Self-employed and currently working": "Self-employed",  # noqa: E501
        },
        "work_status_v2": {
            "Child under 5y not attending child care": "Child under 4-5y not attending child care",  # noqa: E501
            "Child under 5y attending child care": "Child under 4-5y attending child care",  # noqa: E501
            "5y and older in full-time education": "4-5y and older at school/home-school",  # noqa: E501
        },
        "household_been_hospital_last_28_days": been_value_map,
        "household_been_care_home_last_28_days": been_value_map,
        "times_outside_shopping_or_socialising_last_7_days": {
            "None": 0,
            "1": 1,
            "2": 2,
            "3": 3,
            "4": 4,
            "5": 5,
            "6": 6,
            "7 times or more": 7,
        },
    }

    df = assign_isin_list(
        df=df,
        column_name_to_assign="self_isolating",
        reference_column="self_isolating_reason",
        values_list=[
            "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes, you have/have had symptoms",
            "Yes, someone you live with had symptoms",
        ],
        true_false_values=["Yes", "No"],
    )
    df = apply_value_map_multiple_columns(df, column_editing_map)
    df = clean_barcode(df=df, barcode_column="swab_sample_barcode", edited_column="swab_sample_barcode_edited_flag")
    df = clean_barcode(df=df, barcode_column="blood_sample_barcode", edited_column="blood_sample_barcode_edited_flag")
    return df


def pre_generic_digital_transformations(df: DataFrame) -> DataFrame:
    """
    Call transformations to digital data necessary before generic transformations are applied
    """
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 3)
    df = assign_date_from_filename(df, "file_date", "survey_response_source_file")
    df = update_strings_to_sentence_case(df, ["survey_completion_status", "survey_not_completed_reason_code"])
    df = df.withColumn("visit_id", F.col("participant_completion_window_id"))
    df = df.withColumn(
        "swab_manual_entry", F.when(F.col("swab_sample_barcode_user_entered").isNull(), "No").otherwise("Yes")
    )
    df = df.withColumn(
        "blood_manual_entry", F.when(F.col("blood_sample_barcode_user_entered").isNull(), "No").otherwise("Yes")
    )

    df = assign_datetime_from_coalesced_columns_and_log_source(
        df,
        column_name_to_assign="visit_datetime",
        source_reference_column_name="visit_date_type",
        primary_datetime_columns=[
            "swab_taken_datetime",
            "blood_taken_datetime",
            "survey_completed_datetime",
            "survey_last_modified_datetime",
            # "swab_return_date",
            # "blood_return_date",
            # "swab_return_future_date",
            # "blood_return_future_date",
        ],
        secondary_date_columns=[],
        file_date_column="file_date",
        min_date="2022-05-01",
        default_timestamp="12:00:00",
    )
    df = update_column_in_time_window(
        df,
        "digital_survey_collection_mode",
        "survey_completed_datetime",
        "Telephone",
        ["20-05-2022T21:30:00", "25-05-2022 11:00:00"],
    )
    return df


def translate_welsh_survey_responses_version_digital(df: DataFrame) -> DataFrame:
    """
    Call functions to translate welsh survey responses from the cis digital questionnaire
    """
    digital_yes_no_columns = [
        "household_invited_to_digital",
        "household_members_under_2_years_count",
        "consent_nhs_data_share_yn",
        "consent_contact_extra_research_yn",
        "consent_use_of_surplus_blood_samples_yn",
        "consent_blood_samples_if_positive_yn",
        "participant_invited_to_digital",
        "participant_enrolled_digital",
        "opted_out_of_next_window",
        "opted_out_of_blood_next_window",
        "swab_taken",
        "questionnaire_started_no_incentive",
        "swab_returned",
        "blood_taken",
        "blood_returned",
        "work_in_additional_paid_employment",
        "work_nursing_or_residential_care_home",
        "work_direct_contact_patients_or_clients",
        "think_have_covid_symptom_fever",
        "think_have_covid_symptom_headache",
        "think_have_covid_symptom_muscle_ache",
        "think_have_covid_symptom_fatigue",
        "think_have_covid_symptom_nausea_or_vomiting",
        "think_have_covid_symptom_abdominal_pain",
        "think_have_covid_symptom_diarrhoea",
        "think_have_covid_symptom_sore_throat",
        "think_have_covid_symptom_cough",
        "think_have_covid_symptom_shortness_of_breath",
        "think_have_covid_symptom_loss_of_taste",
        "think_have_covid_symptom_loss_of_smell",
        "think_have_covid_symptom_more_trouble_sleeping",
        "think_have_covid_symptom_loss_of_appetite",
        "think_have_covid_symptom_runny_nose_or_sneezing",
        "think_have_covid_symptom_noisy_breathing",
        "think_have_covid_symptom_chest_pain",
        "think_have_covid_symptom_palpitations",
        "think_have_covid_symptom_vertigo_or_dizziness",
        "think_have_covid_symptom_anxiety",
        "think_have_covid_symptom_low_mood",
        "think_have_covid_symptom_memory_loss_or_confusion",
        "think_have_covid_symptom_difficulty_concentrating",
        "self_isolating",
        "think_have_covid",
        "illness_lasting_over_12_months",
        "ever_smoked_regularly",
        "currently_smokes_or_vapes",
        "cis_covid_vaccine_received",
        "cis_covid_vaccine_type_1",
        "cis_covid_vaccine_type_2",
        "cis_covid_vaccine_type_3",
        "cis_covid_vaccine_type_4",
        "cis_covid_vaccine_type_5",
        "cis_covid_vaccine_type_6",
        "cis_flu_vaccine_received",
        "been_outside_uk",
        "think_had_covid",
        "think_had_covid_any_symptoms",
        "think_had_covid_symptom_fever",
        "think_had_covid_symptom_headache",
        "think_had_covid_symptom_muscle_ache",
        "think_had_covid_symptom_fatigue",
        "think_had_covid_symptom_nausea_or_vomiting",
        "think_had_covid_symptom_abdominal_pain",
        "think_had_covid_symptom_diarrhoea",
        "think_had_covid_symptom_sore_throat",
        "think_had_covid_symptom_cough",
        "think_had_covid_symptom_shortness_of_breath",
        "think_had_covid_symptom_loss_of_taste",
        "think_had_covid_symptom_loss_of_smell",
        "think_had_covid_symptom_more_trouble_sleeping",
        "think_had_covid_symptom_loss_of_appetite",
        "think_had_covid_symptom_runny_nose_or_sneezing",
        "think_had_covid_symptom_noisy_breathing",
        "think_had_covid_symptom_chest_pain",
        "think_had_covid_symptom_palpitations",
        "think_had_covid_symptom_vertigo_or_dizziness",
        "think_had_covid_symptom_anxiety",
        "think_had_covid_symptom_low_mood",
        "think_had_covid_symptom_memory_loss_or_confusion",
        "think_had_covid_symptom_difficulty_concentrating",
        "think_had_covid_contacted_nhs",
        "think_had_covid_admitted_to_hospital",
        "other_covid_infection_test",
        "regularly_lateral_flow_testing",
        "other_antibody_test",
        "think_have_long_covid",
        "think_have_long_covid_symptom_fever",
        "think_have_long_covid_symptom_headache",
        "think_have_long_covid_symptom_muscle_ache",
        "think_have_long_covid_symptom_fatigue",
        "think_have_long_covid_symptom_nausea_or_vomiting",
        "think_have_long_covid_symptom_abdominal_pain",
        "think_have_long_covid_symptom_diarrhoea",
        "think_have_long_covid_symptom_loss_of_taste",
        "think_have_long_covid_symptom_loss_of_smell",
        "think_have_long_covid_symptom_sore_throat",
        "think_have_long_covid_symptom_cough",
        "think_have_long_covid_symptom_shortness_of_breath",
        "think_have_long_covid_symptom_loss_of_appetite",
        "think_have_long_covid_symptom_chest_pain",
        "think_have_long_covid_symptom_palpitations",
        "think_have_long_covid_symptom_vertigo_or_dizziness",
        "think_have_long_covid_symptom_anxiety",
        "think_have_long_covid_symptom_low_mood",
        "think_have_long_covid_symptom_more_trouble_sleeping",
        "think_have_long_covid_symptom_memory_loss_or_confusion",
        "think_have_long_covid_symptom_difficulty_concentrating",
        "think_have_long_covid_symptom_runny_nose_or_sneezing",
        "think_have_long_covid_symptom_noisy_breathing",
        "contact_known_positive_covid_last_28_days",
        "hospital_last_28_days",
        "other_household_member_hospital_last_28_days",
        "care_home_last_28_days",
        "other_household_member_care_home_last_28_days",
        "work_main_job_changed",
        "swab_sample_barcode_correct",
        "blood_sample_barcode_correct",
        "think_have_covid_symptoms",
    ]
    df = apply_value_map_multiple_columns(
        df,
        {k: _welsh_yes_no_categories for k in digital_yes_no_columns},
    )
    column_editing_map = {
        "physical_contact_under_18_years": _welsh_contact_type_by_age_group_categories,
        "physical_contact_18_to_69_years": _welsh_contact_type_by_age_group_categories,
        "physical_contact_over_70_years": _welsh_contact_type_by_age_group_categories,
        "social_distance_contact_under_18_years": _welsh_contact_type_by_age_group_categories,
        "social_distance_contact_18_to_69_years": _welsh_contact_type_by_age_group_categories,
        "social_distance_contact_over_70_years": _welsh_contact_type_by_age_group_categories,
        "times_hour_or_longer_another_home_last_7_days": _welsh_number_of_types_categories,
        "times_hour_or_longer_another_person_your_home_last_7_days": _welsh_number_of_types_categories,
        "times_shopping_last_7_days": _welsh_number_of_types_categories,
        "times_socialising_last_7_days": _welsh_number_of_types_categories,
        "cis_covid_vaccine_type": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_number_of_doses": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_type_1": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_type_2": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_type_3": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_type_4": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_type_5": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_type_6": _welsh_vaccination_type_categories,
        "illness_reduces_activity_or_ability": _welsh_lot_little_not_categories,
        "think_have_long_covid_symptom_reduced_ability": _welsh_lot_little_not_categories,
        "last_covid_contact_type": _welsh_live_with_categories,
        "last_suspected_covid_contact_type": _welsh_live_with_categories,
        "face_covering_work_or_education": _welsh_face_covering_categories,
        "face_covering_other_enclosed_places": _welsh_face_covering_categories,
        "swab_not_taken_reason": _welsh_swab_sample_not_taken_categories,
        "blood_not_taken_reason": _welsh_blood_sample_not_taken_categories,
        "work_status_digital": _welsh_work_status_digital_categories,
        "work_status_employment": _welsh_work_status_employment_categories,
        "work_status_unemployment": _welsh_work_status_unemployment_categories,
        "work_status_education": _welsh_work_status_education_categories,
        "work_sector": _welsh_work_sector_categories,
        "work_location": _welsh_work_location_categories,
        "transport_to_work_or_education": _welsh_transport_to_work_education_categories,
        "ability_to_socially_distance_at_work_or_education": _welsh_ability_to_socially_distance_at_work_or_education_categories,
        "self_isolating_reason_detailed": _welsh_self_isolating_reason_detailed_categories,
        "cis_covid_vaccine_number_of_doses": _welsh_cis_covid_vaccine_number_of_doses_categories,
        "other_covid_infection_test_results": _welsh_other_covid_infection_test_result_categories,
        "other_antibody_test_results": _welsh_other_covid_infection_test_result_categories,  # TODO Check translation values in test file
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)

    df = translate_column_regex_replace(
        df, "currently_smokes_or_vapes_description", _welsh_currently_smokes_or_vapes_description_categories
    )
    df = translate_column_regex_replace(df, "blood_not_taken_missing_parts", _welsh_blood_kit_missing_categories)
    df = translate_column_regex_replace(
        df, "blood_not_taken_could_not_reason", _welsh_blood_not_taken_reason_categories
    )
    df = translate_column_regex_replace(df, "swab_not_taken_missing_parts", _welsh_swab_kit_missing_categories)

    return df


def transform_survey_responses_version_digital_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process digital specific variable transformations.
    """
    dont_know_columns = [
        "work_in_additional_paid_employment",
        "work_nursing_or_residential_care_home",
        "work_direct_contact_patients_or_clients",
        "self_isolating",
        "illness_lasting_over_12_months",
        "ever_smoked_regularly",
        "currently_smokes_or_vapes",
        "cis_covid_vaccine_type_1",
        "cis_covid_vaccine_type_2",
        "cis_covid_vaccine_type_3",
        "cis_covid_vaccine_type_4",
        "cis_covid_vaccine_type_5",
        "cis_covid_vaccine_type_6",
        "other_household_member_hospital_last_28_days",
        "other_household_member_care_home_last_28_days",
        "hours_a_day_with_someone_else_at_home",
        "physical_contact_under_18_years",
        "physical_contact_18_to_69_years",
        "physical_contact_over_70_years",
        "social_distance_contact_under_18_years",
        "social_distance_contact_18_to_69_years",
        "social_distance_contact_over_70_years",
        "times_hour_or_longer_another_home_last_7_days",
        "times_hour_or_longer_another_person_your_home_last_7_days",
        "times_shopping_last_7_days",
        "times_socialising_last_7_days",
        "face_covering_work_or_education",
        "face_covering_other_enclosed_places",
        "cis_covid_vaccine_type",
    ]
    df = assign_raw_copies(df, dont_know_columns)
    dont_know_mapping_dict = {
        "Prefer not to say": None,
        "Don't Know": None,
        "I don't know the type": "Don't know type",
        "Dont know": None,
        "Don&#39;t know": None,
        "Do not know": None,
        "Don&amp;#39;t Know": None,
    }
    df = apply_value_map_multiple_columns(
        df,
        {k: dont_know_mapping_dict for k in dont_know_columns},
    )

    df = assign_column_value_from_multiple_column_map(
        df,
        "self_isolating_reason",
        [
            [
                "No",
                ["No", None],
            ],
            [
                "Yes, you have/have had symptoms",
                ["Yes", "I have or have had symptoms of COVID-19 or a positive test"],
            ],
            [
                "Yes, someone you live with had symptoms",
                [
                    "Yes",
                    [
                        "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",  # noqa: E501
                        # TODO: Remove once encoding fixed in raw data
                        "I haven&#39;t had any symptoms but I live with someone who has or has had symptoms or a positive test",  # noqa: E501
                    ],
                ],
            ],
            [
                "Yes, for other reasons (e.g. going into hospital, quarantining)",
                [
                    "Yes",
                    "Due to increased risk of getting COVID-19 such as having been in contact with a known case or quarantining after travel abroad",
                ],  # noqa: E501
            ],
            [
                "Yes, for other reasons (e.g. going into hospital, quarantining)",
                [
                    "Yes",
                    "Due to reducing my risk of getting COVID-19 such as going into hospital or shielding",
                ],  # noqa: E501
            ],
        ],
        ["self_isolating", "self_isolating_reason_detailed"],
    )

    column_list = ["work_status_digital", "work_status_employment", "work_status_unemployment", "work_status_education"]
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v2",
        [
            [
                "Employed and currently working",
                [
                    "Employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Employed and currently not working",
                [
                    "Employed",
                    [
                        "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501
                        "Or currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks?",  # noqa: E501
                    ],
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently working",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently not working",
                [
                    "Self-employed",
                    [
                        "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501
                        "Or currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks?",  # noqa: E501
                    ],
                    None,
                    None,
                ],
            ],
            [
                "Looking for paid work and able to start",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working and not looking for work",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",  # noqa: E501
                    None,
                ],
            ],
            [
                "Retired",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Retired", "Or retired?"],
                    None,
                ],
            ],
            [
                "Child under 4-5y not attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and not attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "Child under 4-5y attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and attending a nursery or a pre-school or childminder",
                ],
            ],
            [
                "4-5y and older at school/home-school",
                [
                    ["In education", None],
                    None,
                    None,
                    ["A child aged 4 or over at school", "A child aged 4 or over at home-school"],
                ],
            ],
            [
                "Attending college or FE (including if temporarily absent)",
                [
                    ["In education", None],
                    None,
                    None,
                    "Attending a college or other further education provider including apprenticeships",
                ],
            ],
            [
                "Attending university (including if temporarily absent)",
                [
                    ["In education", None],
                    None,
                    None,
                    ["Attending university", "Or attending university?"],
                ],
            ],
        ],
        column_list,
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v1",
        [
            [
                "Employed and currently working",
                [
                    "Employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Employed and currently not working",
                [
                    "Employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently working",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently not working",
                [
                    "Self-employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Looking for paid work and able to start",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working and not looking for work",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",
                    None,
                ],
            ],
            [
                "Retired",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Or retired?", "Retired"],
                    None,
                ],
            ],
            [
                "Child under 5y not attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and not attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "Child under 5y attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and attending a nursery or a pre-school or childminder",
                ],
            ],
            [
                "5y and older in full-time education",
                [
                    ["In education", None],
                    None,
                    None,
                    [
                        "A child aged 4 or over at school",
                        "A child aged 4 or over at home-school",
                        "Attending a college or other further education provider including apprenticeships",
                        "Attending university",
                    ],
                ],
            ],
        ],
        column_list,
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v0",
        [
            [
                "Employed",
                [
                    "Employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501
                    None,
                    None,
                ],
            ],
            ["Employed", ["Employed", None, None, None]],
            [
                "Self-employed",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            ["Self-employed", ["Self-employed", None, None, None]],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Self-employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501,
                    None,
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",  # noqa: E501
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Retired", "Or retired?"],
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    None,
                    None,
                ],
            ],
            [
                "Student",
                [
                    ["In education", None],
                    None,
                    None,
                    [
                        "A child below school age and not attending a nursery or pre-school or childminder",
                        "A child below school age and attending a nursery or a pre-school or childminder",
                        "A child aged 4 or over at school",
                        "A child aged 4 or over at home-school",
                        "Attending a college or other further education provider including apprenticeships",
                        "Attending university",
                    ],
                ],
            ],
            ["Student", ["In education", None, None, None]],
        ],
        column_list,
    )
    df = clean_barcode_simple(df, "swab_sample_barcode_user_entered")
    df = clean_barcode_simple(df, "blood_sample_barcode_user_entered")
    df = map_options_to_bool_columns(
        df,
        "currently_smokes_or_vapes_description",
        {
            "Cigarettes": "smoke_cigarettes",
            "Cigars": "smokes_cigar",
            "Pipe": "smokes_pipe",
            "Vape or E-cigarettes": "smokes_vape_e_cigarettes",
            "Hookah or shisha pipes": "smokes_hookah_shisha_pipes",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "blood_not_taken_missing_parts",
        {
            "Small sample test tube. This is the tube that is used to collect the blood.": "blood_not_taken_missing_parts_small_sample_tube",  # noqa: E501
            "Large sample carrier tube with barcode on. This is the tube that you put the small sample test tube in to after collecting blood.": "blood_not_taken_missing_parts_large_sample_carrier",  # noqa: E501
            "Re-sealable biohazard bag with absorbent pad": "blood_not_taken_missing_parts_biohazard_bag",
            "Copy of your blood barcode": "blood_not_taken_missing_parts_blood_barcode",
            "Lancets": "blood_not_taken_missing_parts_lancets",
            "Plasters": "blood_not_taken_missing_parts_plasters",
            "Alcohol wipes": "blood_not_taken_missing_parts_alcohol_wipes",
            "Cleansing wipe": "blood_not_taken_missing_parts_cleansing_wipe",
            "Sample box": "blood_not_taken_missing_parts_sample_box",
            "Sample return bag with a return label on": "blood_not_taken_missing_parts_sample_return_bag",
            "Other please specify": "blood_not_taken_missing_parts_other",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "blood_not_taken_could_not_reason",
        {
            "I couldn't get enough blood into the pot": "blood_not_taken_could_not_reason_not_enough_blood",
            "The pot spilled": "blood_not_taken_could_not_reason_pot_spilled",
            "I had bruising or pain": "blood_not_taken_could_not_reason_had_bruising",
            "I felt unwell": "blood_not_taken_could_not_reason_unwell",
            "Other please specify": "blood_not_taken_could_not_reason_other",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "swab_not_taken_missing_parts",
        {
            "Sample pot with fluid in the bottom and barcode on": "swab_not_taken_missing_parts_sample_pot",
            "Swab stick": "swab_not_taken_missing_parts_swab_stick",
            "Re-sealable biohazard bag with absorbent pad": "swab_not_taken_missing_parts_biohazard_bag",
            "Copy of your swab barcode": "swab_not_taken_missing_parts_swab_barcode",
            "Sample box": "swab_not_taken_missing_parts_sample_box",
            "Sample return bag with a return label on": "swab_not_taken_missing_parts_return_bag",
            "Other please specify": "swab_not_taken_missing_parts_other",
        },
        ";",
    )
    df = df.withColumn("times_outside_shopping_or_socialising_last_7_days", F.lit(None))
    raw_copy_list = [
        "participant_survey_status",
        "participant_withdrawal_type",
        "survey_response_type",
        "work_sector",
        "illness_reduces_activity_or_ability",
        "ability_to_socially_distance_at_work_or_education",
        "last_covid_contact_type",
        "last_suspected_covid_contact_type",
        "physical_contact_under_18_years",
        "physical_contact_18_to_69_years",
        "physical_contact_over_70_years",
        "social_distance_contact_under_18_years",
        "social_distance_contact_18_to_69_years",
        "social_distance_contact_over_70_years",
        "times_hour_or_longer_another_home_last_7_days",
        "times_hour_or_longer_another_person_your_home_last_7_days",
        "times_shopping_last_7_days",
        "times_socialising_last_7_days",
        "face_covering_work_or_education",
        "face_covering_other_enclosed_places",
        "other_covid_infection_test_results",
        "other_antibody_test_results",
        "cis_covid_vaccine_type",
        "cis_covid_vaccine_number_of_doses",
        "cis_covid_vaccine_type_1",
        "cis_covid_vaccine_type_2",
        "cis_covid_vaccine_type_3",
        "cis_covid_vaccine_type_4",
        "cis_covid_vaccine_type_5",
        "cis_covid_vaccine_type_6",
    ]
    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])
    """
    Sets categories to map for digital specific variables to Voyager 0/1/2 equivalent
    """
    contact_people_value_map = {
        "1 to 5": "1-5",
        "6 to 10": "6-10",
        "11 to 20": "11-20",
        "Don't know": None,
        "Prefer not to say": None,
    }
    times_value_map = {
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4,
        "5": 5,
        "6": 6,
        "7 times or more": 7,
        "Don't know": None,
        "None": 0,
        "Prefer not to say": None,
    }
    vaccine_type_map = {
        "Pfizer / BioNTech": "Pfizer/BioNTech",
        "Oxford / AstraZeneca": "Oxford/AstraZeneca",
        "Janssen / Johnson&Johnson": "Janssen/Johnson&Johnson",
        "Another vaccine please specify": "Other / specify",
        "I don't know the type": "Don't know type",
        "Or Another vaccine please specify": "Other / specify",  # changed from "Other /specify"
        "I do not know the type": "Don't know Type",
        "Or do you not know which one you had?": "Don't know Type",
    }
    column_editing_map = {
        "participant_survey_status": {"Complete": "Completed"},
        "participant_withdrawal_type": {
            "Withdrawn - no future linkage": "Withdrawn_no_future_linkage",
            "Withdrawn - no future linkage or use of samples": "Withdrawn_no_future_linkage_or_use_of_samples",
        },
        "survey_response_type": {"First Survey": "First Visit", "Follow-up Survey": "Follow-up Visit"},
        "voucher_type_preference": {"Letter": "Paper", "Email": "email_address"},
        "work_sector": {
            "Social Care": "Social care",
            "Transport. This includes storage and logistics": "Transport (incl. storage, logistic)",
            "Retail sector. This includes wholesale": "Retail sector (incl. wholesale)",
            "Hospitality - for example hotels or restaurants or cafe": "Hospitality (e.g. hotel, restaurant)",
            "Food production and agriculture. This includes farming": "Food production, agriculture, farming",
            "Personal Services - for example hairdressers or tattooists": "Personal services (e.g. hairdressers)",
            "Information technology and communication": "Information technology and communication",
            "Financial services. This includes insurance": "Financial services incl. insurance",
            "Civil Service or Local Government": "Civil service or Local Government",
            "Arts or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Other employment sector please specify": "Other occupation sector",
        },
        "work_health_care_area": {
            "Primary care - for example in a GP or dentist": "Yes, in primary care, e.g. GP, dentist",
            "Secondary care - for example in a hospital": "Yes, in secondary care, e.g. hospital",
            "Another type of healthcare - for example mental health services": "Yes, in other healthcare settings, e.g. mental health",  # noqa: E501
        },
        "illness_reduces_activity_or_ability": {
            "Yes a little": "Yes, a little",
            "Yes a lot": "Yes, a lot",
        },
        "work_location": {
            "From home meaning in the same grounds or building as your home": "Working from home",
            "Somewhere else meaning not at your home)": "Working somewhere else (not your home)",
            "Both from home and work somewhere else": "Both (from home and somewhere else)",
        },
        "transport_to_work_or_education": {
            "Bus or minibus or coach": "Bus, minibus, coach",
            "Motorbike or scooter or moped": "Motorbike, scooter or moped",
            "Taxi or minicab": "Taxi/minicab",
            "Underground or Metro or Light Rail or Tram": "Underground, metro, light rail, tram",
        },
        "ability_to_socially_distance_at_work_or_education": {
            "Difficult to maintain 2 metres apart. But you can usually be at least 1 metre away from other people": "Difficult to maintain 2m, but can be 1m",  # noqa: E501
            "Easy to maintain 2 metres apart. It is not a problem to stay this far away from other people": "Easy to maintain 2m",  # noqa: E501
            "Relatively easy to maintain 2 metres apart. Most of the time you can be 2 meters away from other people": "Relatively easy to maintain 2m",  # noqa: E501
            "Very difficult to be more than 1 metre away. Your work means you are in close contact with others on a regular basis": "Very difficult to be more than 1m away",
        },
        "last_covid_contact_type": {
            "Someone I live with": "Living in your own home",
            "Someone I do not live with": "Outside your home",
        },
        "last_suspected_covid_contact_type": {
            "Someone I live with": "Living in your own home",
            "Someone I do not live with": "Outside your home",
        },
        "physical_contact_under_18_years": contact_people_value_map,
        "physical_contact_18_to_69_years": contact_people_value_map,
        "physical_contact_over_70_years": contact_people_value_map,
        "social_distance_contact_under_18_years": contact_people_value_map,
        "social_distance_contact_18_to_69_years": contact_people_value_map,
        "social_distance_contact_over_70_years": contact_people_value_map,
        "times_hour_or_longer_another_home_last_7_days": times_value_map,
        "times_hour_or_longer_another_person_your_home_last_7_days": times_value_map,
        "times_shopping_last_7_days": times_value_map,
        "times_socialising_last_7_days": times_value_map,
        "face_covering_work_or_education": {
            "Prefer not to say": None,
            "Yes sometimes": "Yes, sometimes",
            "Yes always": "Yes, always",
            "I am not going to my place of work or education": "Not going to place of work or education",
            "I cover my face for other reasons - for example for religious or cultural reasons": "My face is already covered",  # noqa: E501
        },
        "face_covering_other_enclosed_places": {
            "Prefer not to say": None,
            "Yes sometimes": "Yes, sometimes",
            "Yes always": "Yes, always",
            "I am not going to other enclosed public spaces or using public transport": "Not going to other enclosed public spaces or using public transport",  # noqa: E501
            "I cover my face for other reasons - for example for religious or cultural reasons": "My face is already covered",  # noqa: E501
        },
        "other_covid_infection_test_results": {
            "All tests failed": "All Tests failed",
            "One or more tests were negative and none were positive": "Any tests negative, but none positive",
            "One or more tests were positive": "One or more positive test(s)",
        },
        "other_antibody_test_results": {
            "All tests failed": "All Tests failed",
            "One or more tests were negative for antibodies and none were positive": "Any tests negative, but none positive",  # noqa: E501
            "One or more tests were positive for antibodies": "One or more positive test(s)",
        },
        "cis_covid_vaccine_type": vaccine_type_map,
        "cis_covid_vaccine_number_of_doses": {
            "1 dose": "1",
            "2 doses": "2",
            "3 doses": "3 or more",
            "4 doses": "3 or more",
            "5 doses": "3 or more",
            "6 doses or more": "3 or more",
        },
        "cis_covid_vaccine_type_1": vaccine_type_map,
        "cis_covid_vaccine_type_2": vaccine_type_map,
        "cis_covid_vaccine_type_3": vaccine_type_map,
        "cis_covid_vaccine_type_4": vaccine_type_map,
        "cis_covid_vaccine_type_5": vaccine_type_map,
        "cis_covid_vaccine_type_6": vaccine_type_map,
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)

    df = edit_to_sum_or_max_value(
        df=df,
        column_name_to_assign="times_outside_shopping_or_socialising_last_7_days",
        columns_to_sum=[
            "times_shopping_last_7_days",
            "times_socialising_last_7_days",
        ],
        max_value=7,
    )
    df = df.withColumn(
        "work_not_from_home_days_per_week",
        F.greatest("work_not_from_home_days_per_week", "education_in_person_days_per_week"),
    )

    df = df.withColumn("face_covering_outside_of_home", F.lit(None).cast("string"))
    df = concat_fields_if_true(df, "think_had_covid_which_symptoms", "think_had_covid_which_symptom_", "Yes", ";")
    df = concat_fields_if_true(df, "which_symptoms_last_7_days", "think_have_covid_symptom_", "Yes", ";")
    df = concat_fields_if_true(df, "long_covid_symptoms", "think_have_long_covid_symptom_", "Yes", ";")
    df = update_column_values_from_map(
        df,
        "survey_completion_status",
        {
            "In progress": "Partially Completed",
            "Submitted": "Completed",
        },
    )
    df = derive_had_symptom_last_7days_from_digital(
        df,
        "think_have_covid_symptom_any",
        "think_have_covid_symptom_",
        [
            "fever",
            "muscle_ache",
            "fatigue",
            "sore_throat",
            "cough",
            "shortness_of_breath",
            "headache",
            "nausea_or_vomiting",
            "abdominal_pain",
            "diarrhoea",
            "loss_of_taste",
            "loss_of_smell",
        ],
    )
    return df


def transform_survey_responses_generic(df: DataFrame) -> DataFrame:
    """
    Generic transformation steps to be applied to all survey response records.
    """
    raw_copy_list = [
        "think_had_covid_any_symptoms",
        "think_have_covid_symptom_any",
        "work_main_job_title",
        "work_main_job_role",
        "work_health_care_patient_facing",
        "work_health_care_area",
        "work_status_v1",
        "work_status_v2",
        "work_social_care",
        "work_not_from_home_days_per_week",
        "work_location",
        "sex",
        "participant_withdrawal_reason",
        "blood_sample_barcode",
        "swab_sample_barcode",
    ]
    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])
    df = assign_unique_id_column(
        df, "unique_participant_response_id", concat_columns=["visit_id", "participant_id", "visit_datetime"]
    )
    df = assign_date_from_filename(df, "file_date", "survey_response_source_file")
    df = assign_column_regex_match(
        df,
        "bad_email",
        reference_column="email_address",
        pattern=r"/^w+[+.w-]*@([w-]+.)*w+[w-]*.([a-z]{2,4}|d+)$/i",
    )
    df = clean_postcode(df, "postcode")

    consent_cols = ["consent_16_visits", "consent_5_visits", "consent_1_visit"]
    if all(col in df.columns for col in consent_cols):
        df = assign_consent_code(df, "consent_summary", reference_columns=consent_cols)

    df = assign_date_difference(df, "days_since_think_had_covid", "think_had_covid_onset_date", "visit_datetime")
    df = assign_grouped_variable_from_days_since(
        df=df,
        binary_reference_column="think_had_covid",
        days_since_reference_column="days_since_think_had_covid",
        column_name_to_assign="days_since_think_had_covid_group",
    )
    df = df.withColumn("hh_id", F.col("ons_household_id"))
    df = update_column_values_from_map(
        df,
        "work_not_from_home_days_per_week",
        {"NA": "99", "N/A (not working/in education etc)": "99", "up to 1": "0.5"},
    )
    if "study_cohort" not in df.columns:
        df = df.withColumn("study_cohort", F.lit("Original"))
    df = df.withColumn(
        "study_cohort", F.when(F.col("study_cohort").isNull(), "Original").otherwise(F.col("study_cohort"))
    )

    df = clean_job_description_string(df, "work_main_job_title")
    df = clean_job_description_string(df, "work_main_job_role")
    df = df.withColumn("work_main_job_title_and_role", F.concat_ws(" ", "work_main_job_title", "work_main_job_role"))
    return df


def derive_additional_v1_2_columns(df: DataFrame) -> DataFrame:
    """
    Transformations specific to the v1 and v2 survey responses.
    """
    df = clean_within_range(df, "hours_a_day_with_someone_else_at_home", [0, 24])
    df = df.withColumn("been_outside_uk_last_country", F.upper(F.col("been_outside_uk_last_country")))

    df = assign_work_social_column(
        df,
        "work_social_care",
        "work_sector",
        "work_nursing_or_residential_care_home",
        "work_direct_contact_patients_or_clients",
    )
    df = assign_work_health_care(
        df,
        "work_health_care_patient_facing",
        direct_contact_column="work_direct_contact_patients_or_clients",
        health_care_column="work_health_care_area",
    )

    return df


def derive_age_based_columns(df: DataFrame, column_name_to_assign: str) -> DataFrame:
    """
    Transformations involving participant age.
    """
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_5_intervals",
        map={2: "2-11", 12: "12-19", 20: "20-49", 50: "50-69", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_over_16",
        map={16: "16-49", 50: "50-70", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_7_intervals",
        map={2: "2-11", 12: "12-16", 17: "17-25", 25: "25-34", 35: "35-49", 50: "50-69", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_5_year_intervals",
        map={
            2: "2-4",
            5: "5-9",
            10: "10-14",
            15: "15-19",
            20: "20-24",
            25: "25-29",
            30: "30-34",
            35: "35-39",
            40: "40-44",
            45: "45-49",
            50: "50-54",
            55: "55-59",
            60: "60-64",
            65: "65-69",
            70: "70-74",
            75: "75-79",
            80: "80-84",
            85: "85-89",
            90: "90+",
        },
    )

    return df


def derive_work_status_columns(df: DataFrame) -> DataFrame:

    work_status_dict = {
        "work_status_v0": {
            "5y and older in full-time education": "Student",
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "Student",  # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",  # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed",  # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed",  # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "Student",  # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working (unemployed, retired, long-term sick etc.)",
            "Retired (include doing voluntary work here)": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Looking for paid work and able to start": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Retired": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Attending university (including if temporarily absent)": "Student",  # noqa: E501
            "Not working and not looking for work": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Student",  # noqa: E501
        },
        "work_status_v1": {
            "Child under 5y attending child care": "Child under 5y attending child care",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Child under 5y attending child care",  # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Child under 5y attending child care",  # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Child under 5y not attending child care",  # noqa: E501
            "Child under 5y not attending child care": "Child under 5y not attending child care",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Child under 5y not attending child care",  # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Employed and currently not working",  # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed and currently working",  # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working and not looking for work",  # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working and not looking for work",
            "Not working and not looking for work": "Not working and not looking for work",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",  # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed and currently working",  # noqa: E501
            "Retired (include doing voluntary work here)": "Retired",  # noqa: E501
            "Looking for paid work and able to start": "Looking for paid work and able to start",  # noqa: E501
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "5y and older in full-time education",  # noqa: E501
            "Attending university (including if temporarily absent)": "5y and older in full-time education",  # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "5y and older in full-time education",  # noqa: E501
        },
        "work_status_v2": {
            "Retired (include doing voluntary work here)": "Retired",  # noqa: E501
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "Attending college or FE (including if temporarily absent)",  # noqa: E501
            "Attending university (including if temporarily absent)": "Attending university (including if temporarily absent)",  # noqa: E501
            "Child under 5y attending child care": "Child under 4-5y attending child care",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Child under 4-5y attending child care",  # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Child under 4-5y attending child care",  # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Child under 4-5y not attending child care",  # noqa: E501
            "Child under 5y not attending child care": "Child under 4-5y not attending child care",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Child under 4-5y not attending child care",  # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "4-5y and older at school/home-school",  # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Employed and currently not working",  # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed and currently working",  # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working and not looking for work",  # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working and not looking for work",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",  # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed and currently working",  # noqa: E501
            "5y and older in full-time education": "4-5y and older at school/home-school",  # noqa: E501
        },
    }

    column_list = ["work_status_v0", "work_status_v1"]
    for column in column_list:
        df = df.withColumn(column, F.col("work_status_v2"))
        df = update_column_values_from_map(df=df, column=column, map=work_status_dict[column])

    df = update_column_values_from_map(df=df, column="work_status_v2", map=work_status_dict["work_status_v2"])

    ## Not needed in release 1. Confirm that these are v2-only when pulling them back in, as they should likely be union dependent.
    # df = assign_work_person_facing_now(df, "work_person_facing_now", "work_person_facing_now", "work_social_care")
    # df = assign_column_given_proportion(
    #     df=df,
    #     column_name_to_assign="ever_work_person_facing_or_social_care",
    #     groupby_column="participant_id",
    #     reference_columns=["work_social_care"],
    #     count_if=["Yes, care/residential home, resident-facing", "Yes, other social care, resident-facing"],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_column_given_proportion(
    #     df=df,
    #     column_name_to_assign="ever_care_home_worker",
    #     groupby_column="participant_id",
    #     reference_columns=["work_social_care", "work_nursing_or_residential_care_home"],
    #     count_if=["Yes, care/residential home, resident-facing"],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_column_given_proportion(
    #     df=df,
    #     column_name_to_assign="ever_had_long_term_health_condition",
    #     groupby_column="participant_id",
    #     reference_columns=["illness_lasting_over_12_months"],
    #     count_if=["Yes"],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_ever_had_long_term_health_condition_or_disabled(
    #     df=df,
    #     column_name_to_assign="ever_had_long_term_health_condition_or_disabled",
    #     health_conditions_column="illness_lasting_over_12_months",
    #     condition_impact_column="illness_reduces_activity_or_ability",
    # )
    return df


def clean_survey_responses_version_2(df: DataFrame) -> DataFrame:
    df = map_column_values_to_null(
        df=df,
        value="Participant Would Not/Could Not Answer",
        column_list=[
            "ethnicity",
            "work_sector",
            "work_health_care_area",
            "work_status_v2",
            "work_location",
            "work_direct_contact_patients_or_clients",
            "work_nursing_or_residential_care_home",
            "survey_response_type",
            "household_visit_status",
            "participant_survey_status",
            "self_isolating_reason",
            "ability_to_socially_distance_at_work_or_education",
            "transport_to_work_or_education",
            "face_covering_outside_of_home",
            "face_covering_work_or_education",
            "face_covering_other_enclosed_places",
            "other_antibody_test_location",
            "participant_withdrawal_reason",
            "cis_covid_vaccine_type",
            "cis_covid_vaccine_number_of_doses",
            "work_not_from_home_days_per_week",
            "times_shopping_last_7_days",
            "times_socialising_last_7_days",
        ],
    )

    # Map to digital from raw V2 values, before editing them to V1 below
    df = assign_from_map(
        df,
        "self_isolating_reason_detailed",
        "self_isolating_reason",
        {
            "Yes for other reasons (e.g. going into hospital or quarantining)": "Due to increased risk of getting COVID-19 such as having been in contact with a known case or quarantining after travel abroad",  # noqa: E501
            "Yes for other reasons related to reducing your risk of getting COVID-19 (e.g. going into hospital or shielding)": "Due to reducing my risk of getting COVID-19 such as going into hospital or shielding",  # noqa: E501
            "Yes for other reasons related to you having had an increased risk of getting COVID-19 (e.g. having been in contact with a known case or quarantining after travel abroad)": "Due to increased risk of getting COVID-19 such as having been in contact with a known case or quarantining after travel abroad",  # noqa: E501
            "Yes because you live with someone who has/has had symptoms but you haven’t had them yourself": "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",  # noqa: E501
            "Yes because you live with someone who has/has had symptoms or a positive test but you haven’t had symptoms yourself": "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",  # noqa: E501
            "Yes because you live with someone who has/has had symptoms but you haven't had them yourself": "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",  # noqa: E501
            "Yes because you have/have had symptoms of COVID-19": "I have or have had symptoms of COVID-19 or a positive test",
            "Yes because you have/have had symptoms of COVID-19 or a positive test": "I have or have had symptoms of COVID-19 or a positive test",
        },
    )

    times_value_map = {"None": 0, "1": 1, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7 times or more": 7}
    column_editing_map = {
        "deferred": {"Deferred 1": "Deferred"},
        "work_location": {
            "Work from home (in the same grounds or building as your home)": "Working from home",
            "Working from home (in the same grounds or building as your home)": "Working from home",
            "From home (in the same grounds or building as your home)": "Working from home",
            "Work somewhere else (not your home)": "Working somewhere else (not your home)",
            "Somewhere else (not at your home)": "Working somewhere else (not your home)",
            "Somewhere else (not your home)": "Working somewhere else (not your home)",
            "Both (working from home and working somewhere else)": "Both (from home and somewhere else)",
            "Both (work from home and work somewhere else)": "Both (from home and somewhere else)",
        },
        "times_outside_shopping_or_socialising_last_7_days": times_value_map,
        "times_shopping_last_7_days": times_value_map,
        "times_socialising_last_7_days": times_value_map,
        "work_sector": {
            "Social Care": "Social care",
            "Transport (incl. storage or logistic)": "Transport (incl. storage, logistic)",
            "Transport (incl. storage and logistic)": "Transport (incl. storage, logistic)",
            "Transport (incl. storage and logistics)": "Transport (incl. storage, logistic)",
            "Retail Sector (incl. wholesale)": "Retail sector (incl. wholesale)",
            "Hospitality (e.g. hotel or restaurant or cafe)": "Hospitality (e.g. hotel, restaurant)",
            "Food Production and agriculture (incl. farming)": "Food production, agriculture, farming",
            "Food production and agriculture (incl. farming)": "Food production, agriculture, farming",
            "Personal Services (e.g. hairdressers or tattooists)": "Personal services (e.g. hairdressers)",
            "Information technology and communication": "Information technology and communication",
            "Financial services (incl. insurance)": "Financial services incl. insurance",
            "Financial Services (incl. insurance)": "Financial services incl. insurance",
            "Civil Service or Local Government": "Civil service or Local Government",
            "Arts or Entertainment or Recreation": "Arts,Entertainment or Recreation",
            "Art or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Arts or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Other employment sector (specify)": "Other occupation sector",
            "Other occupation sector (specify)": "Other occupation sector",
        },
        "work_health_care_area": {
            "Primary Care (e.g. GP or dentist)": "Yes, in primary care, e.g. GP, dentist",
            "Primary care (e.g. GP or dentist)": "Yes, in primary care, e.g. GP, dentist",
            "Secondary Care (e.g. hospital)": "Yes, in secondary care, e.g. hospital",
            "Secondary care (e.g. hospital.)": "Yes, in secondary care, e.g. hospital",
            "Secondary care (e.g. hospital)": "Yes, in secondary care, e.g. hospital",
            "Other Healthcare (e.g. mental health)": "Yes, in other healthcare settings, e.g. mental health",
            "Other healthcare (e.g. mental health)": "Yes, in other healthcare settings, e.g. mental health",
        },
        "face_covering_outside_of_home": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes at work/school only": "Yes, at work/school only",
            "Yes in other situations only (including public transport/shops)": "Yes, in other situations only",
            "Yes usually both at work/school and in other situations": "Yes, usually both Work/school/other",
            "Yes in other situations only (including public transport or shops)": "Yes, in other situations only",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "face_covering_other_enclosed_places": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes at work/school only": "Yes, at work/school only",
            "Yes in other situations only (including public transport/shops)": "Yes, in other situations only",
            "Yes usually both at work/school and in other situations": "Yes, usually both Work/school/other",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "face_covering_work_or_education": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "other_antibody_test_results": {
            "One or more negative tests but none positive": "Any tests negative, but none positive",
            "One or more negative tests but none were positive": "Any tests negative, but none positive",
            "All tests failed": "All Tests failed",
        },
        "other_antibody_test_location": {
            "Private Lab": "Private lab",
            "Home Test": "Home test",
            "In the NHS (e.g. GP or hospital)": "In the NHS (e.g. GP, hospital)",
        },
        "other_covid_infection_test_results": {
            "One or more negative tests but none positive": "Any tests negative, but none positive",
            "One or more negative tests but none were positive": "Any tests negative, but none positive",
            "All tests failed": "All Tests failed",
            "Positive": "One or more positive test(s)",
            "Negative": "Any tests negative, but none positive",
            "Void": "All Tests failed",
        },
        "illness_reduces_activity_or_ability": {
            "Yes a little": "Yes, a little",
            "Yes a lot": "Yes, a lot",
            "Participant Would Not/Could Not Answer": None,
        },
        "participant_visit_status": {"Participant did not attend": "Patient did not attend", "Canceled": "Cancelled"},
        "self_isolating_reason": {
            "Yes for other reasons (e.g. going into hospital or quarantining)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes for other reasons related to reducing your risk of getting COVID-19 (e.g. going into hospital or shielding)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes for other reasons related to you having had an increased risk of getting COVID-19 (e.g. having been in contact with a known case or quarantining after travel abroad)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes because you live with someone who has/has had symptoms but you haven’t had them yourself": "Yes, someone you live with had symptoms",
            "Yes because you live with someone who has/has had symptoms or a positive test but you haven’t had symptoms yourself": "Yes, someone you live with had symptoms",
            "Yes because you live with someone who has/has had symptoms but you haven't had them yourself": "Yes, someone you live with had symptoms",
            "Yes because you have/have had symptoms of COVID-19": "Yes, you have/have had symptoms",
            "Yes because you have/have had symptoms of COVID-19 or a positive test": "Yes, you have/have had symptoms",
        },
        "ability_to_socially_distance_at_work_or_education": {
            "Difficult to maintain 2 meters - but I can usually be at least 1m from other people": "Difficult to maintain 2m, but can be 1m",
            "Difficult to maintain 2m - but you can usually be at least 1m from other people": "Difficult to maintain 2m, but can be 1m",
            "Easy to maintain 2 meters - it is not a problem to stay this far away from other people": "Easy to maintain 2m",
            "Easy to maintain 2m - it is not a problem to stay this far away from other people": "Easy to maintain 2m",
            "Relatively easy to maintain 2 meters - most of the time I can be 2m away from other people": "Relatively easy to maintain 2m",
            "Relatively easy to maintain 2m - most of the time you can be 2m away from other people": "Relatively easy to maintain 2m",
            "Very difficult to be more than 1 meter away as my work means I am in close contact with others on a regular basis": "Very difficult to be more than 1m away",
            "Very difficult to be more than 1m away as your work means you are in close contact with others on a regular basis": "Very difficult to be more than 1m away",
        },
        "transport_to_work_or_education": {
            "Bus or Minibus or Coach": "Bus, minibus, coach",
            "Bus or minibus or coach": "Bus, minibus, coach",
            "Bus": "Bus, minibus, coach",
            "Motorbike or Scooter or Moped": "Motorbike, scooter or moped",
            "Motorbike or scooter or moped": "Motorbike, scooter or moped",
            "Car or Van": "Car or van",
            "Taxi/Minicab": "Taxi/minicab",
            "On Foot": "On foot",
            "Underground or Metro or Light Rail or Tram": "Underground, metro, light rail, tram",
            "Other Method": "Other method",
        },
        "last_covid_contact_type": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
        "last_suspected_covid_contact_type": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)
    df = df.withColumn("deferred", F.when(F.col("deferred").isNull(), "NA").otherwise(F.col("deferred")))

    df = df.withColumn("swab_sample_barcode", F.upper(F.col("swab_sample_barcode")))
    df = df.withColumn("blood_sample_barcode", F.upper(F.col("blood_sample_barcode")))
    return df


def transform_survey_responses_version_2_delta(df: DataFrame) -> DataFrame:
    """
    Transformations that are specific to version 2 survey responses.
    """
    df = assign_taken_column(df=df, column_name_to_assign="swab_taken", reference_column="swab_sample_barcode")
    df = assign_taken_column(df=df, column_name_to_assign="blood_taken", reference_column="blood_sample_barcode")

    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 2)

    # After editing to V1 values in cleaning
    df = assign_isin_list(
        df=df,
        column_name_to_assign="self_isolating",
        reference_column="self_isolating_reason",
        values_list=[
            "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes, you have/have had symptoms",
            "Yes, someone you live with had symptoms",
        ],
        true_false_values=["Yes", "No"],
    )
    df = edit_to_sum_or_max_value(
        df=df,
        column_name_to_assign="times_outside_shopping_or_socialising_last_7_days",
        columns_to_sum=[
            "times_shopping_last_7_days",
            "times_socialising_last_7_days",
        ],
        max_value=7,
    )
    df = derive_work_status_columns(df)
    return df


def assign_has_been_columns(df):
    df = derive_household_been_columns(
        df=df,
        column_name_to_assign="household_been_care_home_last_28_days",
        individual_response_column="care_home_last_28_days",
        household_response_column="other_household_member_care_home_last_28_days",
    )
    df = derive_household_been_columns(
        df=df,
        column_name_to_assign="household_been_hospital_last_28_days",
        individual_response_column="hospital_last_28_days",
        household_response_column="other_household_member_hospital_last_28_days",
    )
    return df


def symptom_column_transformations(df):
    df = count_value_occurrences_in_column_subset_row_wise(
        df=df,
        column_name_to_assign="think_have_covid_symptom_count",
        selection_columns=[
            "think_have_covid_symptom_fever",
            "think_have_covid_symptom_muscle_ache",
            "think_have_covid_symptom_fatigue",
            "think_have_covid_symptom_sore_throat",
            "think_have_covid_symptom_cough",
            "think_have_covid_symptom_shortness_of_breath",
            "think_have_covid_symptom_headache",
            "think_have_covid_symptom_nausea_or_vomiting",
            "think_have_covid_symptom_abdominal_pain",
            "think_have_covid_symptom_diarrhoea",
            "think_have_covid_symptom_loss_of_taste",
            "think_have_covid_symptom_loss_of_smell",
            "think_have_covid_symptom_more_trouble_sleeping",
            "think_have_covid_symptom_chest_pain",
            "think_have_covid_symptom_palpitations",
            "think_have_covid_symptom_vertigo_or_dizziness",
            "think_have_covid_symptom_anxiety",
            "think_have_covid_symptom_low_mood",
            "think_have_covid_symptom_memory_loss_or_confusion",
            "think_have_covid_symptom_difficulty_concentrating",
            "think_have_covid_symptom_runny_nose_or_sneezing",
            "think_have_covid_symptom_noisy_breathing",
            "think_have_covid_symptom_loss_of_appetite",
        ],
        count_if_value="Yes",
    )
    df = count_value_occurrences_in_column_subset_row_wise(
        df=df,
        column_name_to_assign="think_had_covid_symptom_count",
        selection_columns=[
            "think_had_covid_symptom_fever",
            "think_had_covid_symptom_muscle_ache",
            "think_had_covid_symptom_fatigue",
            "think_had_covid_symptom_sore_throat",
            "think_had_covid_symptom_cough",
            "think_had_covid_symptom_shortness_of_breath",
            "think_had_covid_symptom_headache",
            "think_had_covid_symptom_nausea_or_vomiting",
            "think_had_covid_symptom_abdominal_pain",
            "think_had_covid_symptom_diarrhoea",
            "think_had_covid_symptom_loss_of_taste",
            "think_had_covid_symptom_loss_of_smell",
            "think_had_covid_symptom_more_trouble_sleeping",
            "think_had_covid_symptom_chest_pain",
            "think_had_covid_symptom_palpitations",
            "think_had_covid_symptom_vertigo_or_dizziness",
            "think_had_covid_symptom_anxiety",
            "think_had_covid_symptom_low_mood",
            "think_had_covid_symptom_memory_loss_or_confusion",
            "think_had_covid_symptom_difficulty_concentrating",
            "think_had_covid_symptom_runny_nose_or_sneezing",
            "think_had_covid_symptom_noisy_breathing",
            "think_had_covid_symptom_loss_of_appetite",
        ],
        count_if_value="Yes",
    )
    # TODO - not needed until later release
    # df = update_think_have_covid_symptom_any(
    #     df=df,
    #     column_name_to_update="think_have_covid_symptom_any",
    #     count_reference_column="think_have_covid_symptom_count",
    # )

    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="any_think_have_covid_symptom_or_now",
    #     reference_columns=["think_have_covid_symptom_any", "think_have_covid"],
    #     true_false_values=["Yes", "No"],
    # )

    # df = assign_any_symptoms_around_visit(
    #     df=df,
    #     column_name_to_assign="any_symptoms_around_visit",
    #     symptoms_bool_column="any_think_have_covid_symptom_or_now",
    #     id_column="participant_id",
    #     visit_date_column="visit_datetime",
    #     visit_id_column="visit_id",
    # )

    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
    #     reference_columns=[
    #         "think_have_covid_symptom_cough",
    #         "think_have_covid_symptom_fever",
    #         "think_have_covid_symptom_loss_of_smell",
    #         "think_have_covid_symptom_loss_of_taste",
    #     ],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
    #     reference_columns=[
    #         "think_had_covid_symptom_cough",
    #         "think_had_covid_symptom_fever",
    #         "think_had_covid_symptom_loss_of_smell",
    #         "think_had_covid_symptom_loss_of_taste",
    #     ],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
    #     reference_columns=[
    #         "think_had_covid_symptom_cough",
    #         "think_had_covid_symptom_fever",
    #         "think_had_covid_symptom_loss_of_smell",
    #         "think_had_covid_symptom_loss_of_taste",
    #     ],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_any_symptoms_around_visit(
    #     df=df,
    #     column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
    #     id_column="participant_id",
    #     symptoms_bool_column="think_have_covid_cghfevamn_symptom_group",
    #     visit_date_column="visit_datetime",
    #     visit_id_column="visit_id",
    # )
    return df


def union_dependent_cleaning(df):
    col_val_map = {
        "ethnicity": {
            "African": "Black,Caribbean,African-African",
            "Caribbean": "Black,Caribbean,Afro-Caribbean",
            "Any other Black or African or Caribbean background": "Any other Black background",
            "Any other Black| African| Carribbean": "Any other Black background",
            "Any other Mixed/Multiple background": "Any other Mixed background",
            "Bangladeshi": "Asian or Asian British-Bangladeshi",
            "Chinese": "Asian or Asian British-Chinese",
            "English, Welsh, Scottish, Northern Irish or British": "White-British",
            "English| Welsh| Scottish| Northern Irish or British": "White-British",
            "Indian": "Asian or Asian British-Indian",
            "Irish": "White-Irish",
            "Pakistani": "Asian or Asian British-Pakistani",
            "White and Asian": "Mixed-White & Asian",
            "White and Black African": "Mixed-White & Black African",
            "White and Black Caribbean": "Mixed-White & Black Caribbean",
            "Roma": "White-Gypsy or Irish Traveller",
            "White-Roma": "White-Gypsy or Irish Traveller",
            "Gypsy or Irish Traveller": "White-Gypsy or Irish Traveller",
            "Arab": "Other ethnic group-Arab",
            "Any other white": "Any other white background",
        },
        "participant_withdrawal_reason": {
            "Bad experience with tester / survey": "Bad experience with interviewer/survey",
            "Swab / blood process too distressing": "Swab/blood process too distressing",
            "Swab / blood process to distressing": "Swab/blood process too distressing",
            "Do NOT Reinstate": "Do not reinstate",
        },
    }
    if "blood_consolidation_point_error" in df.columns:  # TEMP DELETE AFTER CONSOLIDATION PREFIX REMOVED
        df = df.withColumn(
            "blood_consolidation_point_error",
            F.regexp_replace(F.col("blood_consolidation_point_error"), r"^[Cc]onsolidation\.", ""),
        )
        df = df.withColumn(
            "swab_consolidation_point_error",
            F.regexp_replace(F.col("swab_consolidation_point_error"), r"^[Cc]onsolidation\.", ""),
        )

    df = apply_value_map_multiple_columns(df, col_val_map)
    df = convert_null_if_not_in_list(df, "sex", options_list=["Male", "Female"])
    # TODO: Add in once dependencies are derived
    # df = impute_latest_date_flag(
    #     df=df,
    #     participant_id_column="participant_id",
    #     visit_date_column="visit_date",
    #     visit_id_column="visit_id",
    #     contact_any_covid_column="contact_known_or_suspected_covid",
    #     contact_any_covid_date_column="contact_known_or_suspected_covid_latest_date",
    # )

    # TODO: Add in once dependencies are derived
    # df = assign_date_difference(
    #     df,
    #     "contact_known_or_suspected_covid_days_since",
    #     "contact_known_or_suspected_covid_latest_date",
    #     "visit_datetime",
    # )

    # TODO: add the following function once contact_known_or_suspected_covid_latest_date() is created
    # df = contact_known_or_suspected_covid_type(
    #     df=df,
    #     contact_known_covid_type_column='contact_known_covid_type',
    #     contact_any_covid_type_column='contact_any_covid_type',
    #     contact_any_covid_date_column='contact_any_covid_date',
    #     contact_known_covid_date_column='contact_known_covid_date',
    #     contact_suspect_covid_date_column='contact_suspect_covid_date',
    # )

    df = update_face_covering_outside_of_home(
        df=df,
        column_name_to_update="face_covering_outside_of_home",
        covered_enclosed_column="face_covering_other_enclosed_places",
        covered_work_column="face_covering_work_or_education",
    )

    return df


def union_dependent_derivations(df):
    """
    Transformations that must be carried out after the union of the different survey response schemas.
    """
    df = assign_fake_id(df, "ordered_household_id", "ons_household_id")
    df = assign_visit_order(
        df=df,
        column_name_to_assign="visit_order",
        id="participant_id",
        order_list=["visit_datetime", "visit_id"],
    )
    df = symptom_column_transformations(df)
    if "survey_completion_status" in df.columns:
        df = df.withColumn(
            "participant_visit_status", F.coalesce(F.col("participant_visit_status"), F.col("survey_completion_status"))
        )
    ethnicity_map = {
        "White": ["White-British", "White-Irish", "White-Gypsy or Irish Traveller", "Any other white background"],
        "Asian": [
            "Asian or Asian British-Indian",
            "Asian or Asian British-Pakistani",
            "Asian or Asian British-Bangladeshi",
            "Asian or Asian British-Chinese",
            "Any other Asian background",
        ],
        "Black": ["Black,Caribbean,African-African", "Black,Caribbean,Afro-Caribbean", "Any other Black background"],
        "Mixed": [
            "Mixed-White & Black Caribbean",
            "Mixed-White & Black African",
            "Mixed-White & Asian",
            "Any other Mixed background",
        ],
        "Other": ["Other ethnic group-Arab", "Any other ethnic group"],
    }
    if "swab_sample_barcode_user_entered" in df.columns:
        for test_type in ["swab", "blood"]:
            df = df.withColumn(
                f"{test_type}_sample_barcode_combined",
                F.when(
                    F.col(f"{test_type}_sample_barcode_correct") == "No",
                    F.col(f"{test_type}_sample_barcode_user_entered"),
                ).otherwise(F.col(f"{test_type}_sample_barcode"))
                # set to sample_barcode if _sample_barcode_correct is yes or null.
            )
    df = assign_column_from_mapped_list_key(
        df=df, column_name_to_assign="ethnicity_group", reference_column="ethnicity", map=ethnicity_map
    )
    df = assign_ethnicity_white(
        df, column_name_to_assign="ethnicity_white", ethnicity_group_column_name="ethnicity_group"
    )
    # df = assign_work_patient_facing_now(
    #     df, "work_patient_facing_now", age_column="age_at_visit", work_healthcare_column="work_health_care_patient_facing"
    # )
    # df = update_work_facing_now_column(
    #     df,
    #     "work_patient_facing_now",
    #     "work_status_v0",
    #     ["Furloughed (temporarily not working)", "Not working (unemployed, retired, long-term sick etc.)", "Student"],
    # )
    # df = assign_first_visit(
    #     df=df,
    #     column_name_to_assign="household_first_visit_datetime",
    #     id_column="participant_id",
    #     visit_date_column="visit_datetime",
    # )
    # df = assign_last_visit(
    #     df=df,
    #     column_name_to_assign="last_attended_visit_datetime",
    #     id_column="participant_id",
    #     visit_status_column="participant_visit_status",
    #     visit_date_column="visit_datetime",
    # )
    # df = assign_date_difference(
    #     df=df,
    #     column_name_to_assign="days_since_enrolment",
    #     start_reference_column="household_first_visit_datetime",
    #     end_reference_column="last_attended_visit_datetime",
    # )
    # df = assign_date_difference(
    #     df=df,
    #     column_name_to_assign="household_weeks_since_survey_enrolment",
    #     start_reference_column="survey start",
    #     end_reference_column="visit_datetime",
    #     format="weeks",
    # )
    # df = assign_named_buckets(
    #     df,
    #     reference_column="days_since_enrolment",
    #     column_name_to_assign="visit_number",
    #     map={
    #         0: 0,
    #         4: 1,
    #         11: 2,
    #         18: 3,
    #         25: 4,
    #         43: 5,
    #         71: 6,
    #         99: 7,
    #         127: 8,
    #         155: 9,
    #         183: 10,
    #         211: 11,
    #         239: 12,
    #         267: 13,
    #         295: 14,
    #         323: 15,
    #     },
    # )
    # df = assign_any_symptoms_around_visit(
    #     df=df,
    #     column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
    #     symptoms_bool_column="think_have_covid_cghfevamn_symptom_group",
    #     id_column="participant_id",
    #     visit_date_column="visit_datetime",
    #     visit_id_column="visit_id",
    # )
    df = derive_people_in_household_count(df)
    df = update_column_values_from_map(
        df=df,
        column="smokes_nothing_now",
        map={"Yes": "No", "No": "Yes"},
        condition_column="currently_smokes_or_vapes",
    )
    df = add_pattern_matching_flags(df)
    df = fill_backwards_work_status_v2(
        df=df,
        date="visit_datetime",
        id="participant_id",
        fill_backward_column="work_status_v2",
        condition_column="work_status_v1",
        date_range=["2020-09-01", "2021-08-31"],
        condition_column_values=["5y and older in full-time education"],
        fill_only_backward_column_values=[
            "4-5y and older at school/home-school",
            "Attending college or FE (including if temporarily absent)",
            "Attending university (including if temporarily absent)",
        ],
    )
    df = assign_work_status_group(df, "work_status_group", "work_status_v0")

    df = fill_forward_from_last_change(
        df=df,
        fill_forward_columns=[
            "cis_covid_vaccine_date",
            "cis_covid_vaccine_number_of_doses",
            "cis_covid_vaccine_type",
            "cis_covid_vaccine_type_other",
            "cis_covid_vaccine_received",
        ],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        record_changed_column="cis_covid_vaccine_received",
        record_changed_value="Yes",
    )
    # Derive these after fill forwards and other changes to dates
    df = create_formatted_datetime_string_columns(df)
    return df


def derive_people_in_household_count(df):
    """
    Correct counts of household member groups and sum to get total number of people in household. Takes maximum
    final count by household for each record.
    """
    df = assign_household_participant_count(
        df,
        column_name_to_assign="household_participant_count",
        household_id_column="ons_household_id",
        participant_id_column="participant_id",
    )
    df = update_person_count_from_ages(
        df,
        column_name_to_assign="household_participants_not_consenting_count",
        column_pattern=r"person_not_consenting_age_[1-9]",
    )
    df = update_person_count_from_ages(
        df,
        column_name_to_assign="household_members_over_2_years_and_not_present_count",
        column_pattern=r"person_not_present_age_[1-8]",
    )
    df = assign_household_under_2_count(
        df,
        column_name_to_assign="household_members_under_2_years_count",
        column_pattern=r"infant_age_months_[1-9]",
        condition_column="household_members_under_2_years",
    )
    household_window = Window.partitionBy("ons_household_id")

    household_participants = [
        "household_participant_count",
        "household_participants_not_consenting_count",
        "household_members_over_2_years_and_not_present_count",
        "household_members_under_2_years_count",
    ]
    for household_participant_type in household_participants:
        df = df.withColumn(
            household_participant_type,
            F.max(household_participant_type).over(household_window),
        )
    df = df.withColumn(
        "people_in_household_count",
        sum_within_row(household_participants),
    )
    df = df.withColumn(
        "people_in_household_count_group",
        F.when(F.col("people_in_household_count") >= 5, "5+").otherwise(
            F.col("people_in_household_count").cast("string")
        ),
    )
    return df


def create_formatted_datetime_string_columns(df):
    """
    Create columns with specific datetime formatting for use in output data.
    """
    date_format_dict = {
        "visit_date_string": "visit_datetime",
        "samples_taken_date_string": "samples_taken_datetime",
    }
    datetime_format_dict = {
        "visit_datetime_string": "visit_datetime",
        "samples_taken_datetime_string": "samples_taken_datetime",
    }
    date_format_string_list = [
        "date_of_birth",
        "improved_visit_date",
        "think_had_covid_onset_date",
        "cis_covid_vaccine_date",
        "cis_covid_vaccine_date_1",
        "cis_covid_vaccine_date_2",
        "cis_covid_vaccine_date_3",
        "cis_covid_vaccine_date_4",
        "last_suspected_covid_contact_date",
        "last_covid_contact_date",
        "other_covid_infection_test_first_positive_date",
        "other_antibody_test_last_negative_date",
        "other_antibody_test_first_positive_date",
        "other_covid_infection_test_last_negative_date",
        "been_outside_uk_last_return_date",
        "think_have_covid_onset_date",
        "swab_return_date",
        "swab_return_future_date",
        "blood_return_date",
        "blood_return_future_date",
        "cis_covid_vaccine_date_5",
        "cis_covid_vaccine_date_6",
        "cis_covid_vaccine_date",
        "think_have_covid_symptom_onset_date",  # tempvar
        "other_covid_infection_test_positive_date",  # tempvar
        "other_covid_infection_test_negative_date",  # tempvar
        "other_antibody_test_positive_date",  # tempvar
        "other_antibody_test_negative_date",  # tempvar
    ]
    date_format_string_list = [
        col for col in date_format_string_list if col not in cis_digital_datetime_map["yyyy-MM-dd"]
    ] + cis_digital_datetime_map["yyyy-MM-dd"]

    for column_name_to_assign, timestamp_column in date_format_dict.items():
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=column_name_to_assign,
                reference_column=timestamp_column,
                time_format="ddMMMyyyy",
                lower_case=True,
            )
    for timestamp_column in date_format_string_list:
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=timestamp_column + "_string",
                reference_column=timestamp_column,
                time_format="ddMMMyyyy",
                lower_case=True,
            )
    for column_name_to_assign, timestamp_column in datetime_format_dict.items():
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=column_name_to_assign,
                reference_column=timestamp_column,
                time_format="ddMMMyyyy HH:mm:ss",
                lower_case=True,
            )
    for timestamp_column in cis_digital_datetime_map["yyyy-MM-dd'T'HH:mm:ss'Z'"]:
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=timestamp_column + "_string",
                reference_column=timestamp_column,
                time_format="ddMMMyyyy HH:mm:ss",
                lower_case=True,
            )
    return df


def transform_from_lookups(
    df: DataFrame, cohort_lookup: DataFrame, travel_countries_lookup: DataFrame, tenure_group: DataFrame
):
    cohort_lookup = cohort_lookup.withColumnRenamed("participant_id", "cohort_participant_id")
    df = df.join(
        F.broadcast(cohort_lookup),
        how="left",
        on=((df.participant_id == cohort_lookup.cohort_participant_id) & (df.study_cohort == cohort_lookup.old_cohort)),
    ).drop("cohort_participant_id")
    df = df.withColumn("study_cohort", F.coalesce(F.col("new_cohort"), F.col("study_cohort"))).drop(
        "new_cohort", "old_cohort"
    )
    df = df.join(
        F.broadcast(travel_countries_lookup.withColumn("REPLACE_COUNTRY", F.lit(True))),
        how="left",
        on=df.been_outside_uk_last_country == travel_countries_lookup.been_outside_uk_last_country_old,
    )
    df = df.withColumn(
        "been_outside_uk_last_country",
        F.when(F.col("REPLACE_COUNTRY"), F.col("been_outside_uk_last_country_new")).otherwise(
            F.col("been_outside_uk_last_country"),
        ),
    ).drop("been_outside_uk_last_country_old", "been_outside_uk_last_country_new", "REPLACE_COUNTRY")

    for key, value in column_name_maps["tenure_group_variable_map"].items():
        tenure_group = tenure_group.withColumnRenamed(key, value)

    df = df.join(tenure_group, on=(df["ons_household_id"] == tenure_group["UAC"]), how="left").drop("UAC")
    return df


def fill_forwards_transformations(df):

    df = fill_forward_from_last_change_marked_subset(
        df=df,
        fill_forward_columns=[
            "work_main_job_title",
            "work_main_job_role",
            "work_sector",
            "work_sector_other",
            "work_social_care",
            "work_health_care_patient_facing",
            "work_health_care_area",
            "work_nursing_or_residential_care_home",
            "work_direct_contact_patients_or_clients",
        ],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        record_changed_column="work_main_job_changed",
        record_changed_value="Yes",
        dateset_version_column="survey_response_dataset_major_version",
        minimum_dateset_version=2,
    )

    # TODO: uncomment for releases after R1
    # df = fill_backwards_overriding_not_nulls(
    #     df=df,
    #     column_identity="participant_id",
    #     ordering_column="visit_date",
    #     dataset_column="survey_response_dataset_major_version",
    #     column_list=fill_forwards_and_then_backwards_list,
    # )

    ## TODO: Not needed until a future release, will leave commented out in code until required
    #
    #    df = update_column_if_ref_in_list(
    #        df=df,
    #        column_name_to_update="work_location",
    #        old_value=None,
    #        new_value="Not applicable, not currently working",
    #        reference_column="work_status_v0",
    #        check_list=[
    #            "Furloughed (temporarily not working)",
    #            "Not working (unemployed, retired, long-term sick etc.)",
    #            "Student",
    #        ],
    #    )

    df = fill_forwards_travel_column(df)

    df = fill_backwards_overriding_not_nulls(
        df=df,
        column_identity="participant_id",
        ordering_column="visit_datetime",
        dataset_column="survey_response_dataset_major_version",
        column_list=["sex", "date_of_birth", "ethnicity"],
    )
    df = fill_forward_only_to_nulls(
        df=df,
        id="participant_id",
        date="visit_datetime",
        list_fill_forward=[
            "sex",
            "date_of_birth",
            "ethnicity",
        ],
    )
    return df


def fill_forwards_travel_column(df):
    df = update_to_value_if_any_not_null(
        df=df,
        column_name_to_assign="been_outside_uk",
        value_to_assign="Yes",
        column_list=["been_outside_uk_last_country", "been_outside_uk_last_return_date"],
    )
    df = fill_forward_from_last_change(
        df=df,
        fill_forward_columns=[
            "been_outside_uk_last_country",
            "been_outside_uk_last_return_date",
            "been_outside_uk",
        ],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        record_changed_column="been_outside_uk",
        record_changed_value="Yes",
    )
    return df


def impute_key_columns(df: DataFrame, imputed_value_lookup_df: DataFrame, log_directory: str):
    """
    Impute missing values for key variables that are required for weight calibration.
    Most imputations require geographic data being joined onto the response records.

    Returns a single record per participant, with response values (when available) and missing values imputed.
    """
    unique_id_column = "participant_id"

    # Get latest record for each participant, assumes that they have been filled forwards
    participant_window = Window.partitionBy(unique_id_column).orderBy(F.col("visit_datetime").desc())
    deduplicated_df = (
        df.withColumn("ROW_NUMBER", F.row_number().over(participant_window))
        .filter(F.col("ROW_NUMBER") == 1)
        .drop("ROW_NUMBER")
    )

    if imputed_value_lookup_df is not None:
        deduplicated_df = merge_previous_imputed_values(deduplicated_df, imputed_value_lookup_df, unique_id_column)

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_mode,
        reference_column="ethnicity_white",
        group_by_column="ons_household_id",
    ).custom_checkpoint()

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_by_k_nearest_neighbours,
        reference_column="ethnicity_white",
        donor_group_columns=["cis_area_code_20"],
        donor_group_column_weights=[5000],
        log_file_path=log_directory,
    ).custom_checkpoint()

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_distribution,
        reference_column="sex",
        group_by_columns=["ethnicity_white", "region_code"],
        first_imputation_value="Female",
        second_imputation_value="Male",
    ).custom_checkpoint()

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_date_by_k_nearest_neighbours,
        reference_column="date_of_birth",
        donor_group_columns=["region_code", "people_in_household_count_group", "work_status_group"],
        log_file_path=log_directory,
    )

    return deduplicated_df.select(
        unique_id_column,
        *["ethnicity_white", "sex", "date_of_birth"],
        *[col for col in deduplicated_df.columns if col.endswith("_imputation_method")],
        *[col for col in deduplicated_df.columns if col.endswith("_is_imputed")],
    )


def nims_transformations(df: DataFrame) -> DataFrame:
    """Clean and transform NIMS data after reading from table."""
    df = rename_column_names(df, column_name_maps["nims_column_name_map"])
    df = assign_column_to_date_string(df, "nims_vaccine_dose_1_date", reference_column="nims_vaccine_dose_1_datetime")
    df = assign_column_to_date_string(df, "nims_vaccine_dose_2_date", reference_column="nims_vaccine_dose_2_datetime")

    # TODO: Derive nims_linkage_status, nims_vaccine_classification, nims_vaccine_dose_1_time, nims_vaccine_dose_2_time
    return df


def derive_overall_vaccination(df: DataFrame) -> DataFrame:
    """Derive overall vaccination status from NIMS and CIS data."""
    return df


def add_pattern_matching_flags(df: DataFrame) -> DataFrame:
    """Add result of various regex pattern matchings"""

    # # add work from home flag
    # df = assign_regex_match_result(
    #     df=df,
    #     columns_to_check_in=["work_main_job_title", "work_main_job_role"],
    #     positive_regex_pattern=work_from_home_pattern.positive_regex_pattern,
    #     negative_regex_pattern=work_from_home_pattern.negative_regex_pattern,
    #     column_name_to_assign="is_working_from_home",
    #     debug_mode=False,
    # )

    # # add at-school flag
    # df = assign_regex_match_result(
    #     df=df,
    #     columns_to_check_in=["work_main_job_title", "work_main_job_role"],
    #     positive_regex_pattern=at_school_pattern.positive_regex_pattern,
    #     negative_regex_pattern=at_school_pattern.negative_regex_pattern,
    #     column_name_to_assign="at_school",
    #     debug_mode=False,
    # )

    # # add at-university flag
    # df = assign_regex_match_result(
    #     df=df,
    #     columns_to_check_in=["work_main_job_title", "work_main_job_role"],
    #     positive_regex_pattern=at_university_pattern.positive_regex_pattern,
    #     negative_regex_pattern=at_university_pattern.negative_regex_pattern,
    #     column_name_to_assign="at_university",
    #     debug_mode=False,
    # )
    # # add is-retired flag
    # df = assign_regex_match_result(
    #     df=df,
    #     columns_to_check_in=["work_main_job_title", "work_main_job_role"],
    #     positive_regex_pattern=retired_regex_pattern.positive_regex_pattern,
    #     negative_regex_pattern=retired_regex_pattern.negative_regex_pattern,
    #     column_name_to_assign="is_retired",
    #     debug_mode=False,
    # )

    # # add not-working flag
    # df = assign_regex_match_result(
    #     df=df,
    #     columns_to_check_in=["work_main_job_title", "work_main_job_role"],
    #     positive_regex_pattern=not_working_pattern.positive_regex_pattern,
    #     negative_regex_pattern=not_working_pattern.negative_regex_pattern,
    #     column_name_to_assign="not_working",
    # )
    # # add self-employed flag
    # df = assign_regex_match_result(
    #     df=df,
    #     columns_to_check_in=["work_main_job_title", "work_main_job_role"],
    #     positive_regex_pattern=self_employed_regex.positive_regex_pattern,
    #     column_name_to_assign="is_self_employed",
    #     debug_mode=False,
    # )
    df = assign_regex_match_result(
        df=df,
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        column_name_to_assign="is_patient_facing",
        positive_regex_pattern=patient_facing_pattern.positive_regex_pattern,
        negative_regex_pattern=patient_facing_pattern.negative_regex_pattern,
    )
    df = assign_regex_match_result(
        df=df,
        positive_regex_pattern=social_care_pattern.positive_regex_pattern,
        negative_regex_pattern=social_care_pattern.negative_regex_pattern,
        column_name_to_assign="works_social_care",
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
    )
    df = assign_regex_match_result(
        df=df,
        positive_regex_pattern=healthcare_pattern.positive_regex_pattern,
        negative_regex_pattern=healthcare_pattern.negative_regex_pattern,
        column_name_to_assign="works_healthcare",
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
    )
    df = df.withColumn(
        "is_patient_facing", F.when(F.col("works_healthcare"), F.col("is_patient_facing")).otherwise(False)
    )
    # df = assign_regex_match_result(
    #     df=df,
    #     positive_regex_pattern=healthcare_bin_pattern.positive_regex_pattern,
    #     negative_regex_pattern=healthcare_bin_pattern.negative_regex_pattern,
    #     column_name_to_assign="works_healthcare_bin",
    #     columns_to_check_in=["work_main_job_title", "work_main_job_role"],
    # )

    window = Window.partitionBy("participant_id")
    df = df.withColumn(
        "patient_facing_over_20_percent",
        F.sum(F.when(F.col("is_patient_facing"), 1).otherwise(0)).over(window) / F.sum(F.lit(1)).over(window),
    )
    return df


def flag_records_to_reclassify(df: DataFrame) -> DataFrame:
    """
    Adds various flags to indicate which rules were triggered for a given record.
    """
    # Work from Home rules
    df = df.withColumn("wfh_rules", flag_records_for_work_from_home_rules())

    # Furlough rules
    df = df.withColumn("furlough_rules_v0", flag_records_for_furlough_rules_v0())

    df = df.withColumn("furlough_rules_v1_a", flag_records_for_furlough_rules_v1_a())

    df = df.withColumn("furlough_rules_v1_b", flag_records_for_furlough_rules_v1_b())

    df = df.withColumn("furlough_rules_v2_a", flag_records_for_furlough_rules_v2_a())

    df = df.withColumn("furlough_rules_v2_b", flag_records_for_furlough_rules_v2_b())

    # Self-employed rules
    df = df.withColumn("self_employed_rules_v1_a", flag_records_for_self_employed_rules_v1_a())

    df = df.withColumn("self_employed_rules_v1_b", flag_records_for_self_employed_rules_v1_b())

    df = df.withColumn("self_employed_rules_v2_a", flag_records_for_self_employed_rules_v2_a())

    df = df.withColumn("self_employed_rules_v2_b", flag_records_for_self_employed_rules_v2_b())

    # Retired rules
    df = df.withColumn("retired_rules_generic", flag_records_for_retired_rules())

    # Not-working rules
    df = df.withColumn("not_working_rules_v0", flag_records_for_not_working_rules_v0())

    df = df.withColumn("not_working_rules_v1_a", flag_records_for_not_working_rules_v1_a())

    df = df.withColumn("not_working_rules_v1_b", flag_records_for_not_working_rules_v1_b())

    df = df.withColumn("not_working_rules_v2_a", flag_records_for_not_working_rules_v2_a())

    df = df.withColumn("not_working_rules_v2_b", flag_records_for_not_working_rules_v2_b())

    # Student rules
    df = df.withColumn("student_rules_v0", flag_records_for_student_v0_rules())

    df = df.withColumn("student_rules_v1", flag_records_for_student_v1_rules())

    df = df.withColumn("school_rules_v2", flag_records_for_school_v2_rules())

    # University rules
    df = df.withColumn("uni_rules_v2", flag_records_for_uni_v2_rules())

    df = df.withColumn("college_rules_v2", flag_records_for_college_v2_rules())

    return df


def reclassify_work_variables(
    df: DataFrame, spark_session: SparkSession, drop_original_variables: bool = True
) -> DataFrame:
    """
    Reclassify work-related variables based on rules & regex patterns

    Parameters
    ----------
    df
        The dataframe containing the work-status related variables we want to edit
    spark_session
        A active spark session - this is used to break lineage since the code generated
        in this function is very verbose, you may encounter memory error if we don't break
        lineage.
    drop_original_variables
        Set this to False if you want to retain the original variables so you can compare
        before & after edits.
    """
    # Work from Home
    update_work_location = flag_records_for_work_from_home_rules() & regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=work_from_home_pattern.positive_regex_pattern,
        negative_regex_pattern=work_from_home_pattern.negative_regex_pattern,
    )

    # Furlough
    furlough_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=furloughed_pattern.positive_regex_pattern,
        negative_regex_pattern=furloughed_pattern.negative_regex_pattern,
    )

    update_work_status_furlough_v0 = furlough_regex_hit & flag_records_for_furlough_rules_v0()
    update_work_status_furlough_v1_a = furlough_regex_hit & flag_records_for_furlough_rules_v1_a()
    update_work_status_furlough_v1_b = furlough_regex_hit & flag_records_for_furlough_rules_v1_b()
    update_work_status_furlough_v2_a = furlough_regex_hit & flag_records_for_furlough_rules_v2_a()
    update_work_status_furlough_v2_b = furlough_regex_hit & flag_records_for_furlough_rules_v2_b()

    # Self-Employed
    self_employed_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=self_employed_regex.positive_regex_pattern,
        negative_regex_pattern=self_employed_regex.negative_regex_pattern,
    )

    update_work_status_self_employed_v1_a = self_employed_regex_hit & flag_records_for_self_employed_rules_v1_a()
    update_work_status_self_employed_v1_b = self_employed_regex_hit & flag_records_for_self_employed_rules_v1_b()
    update_work_status_self_employed_v2_a = self_employed_regex_hit & flag_records_for_self_employed_rules_v2_a()
    update_work_status_self_employed_v2_b = self_employed_regex_hit & flag_records_for_self_employed_rules_v2_b()

    # Retired
    retired_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=retired_regex_pattern.positive_regex_pattern,
        negative_regex_pattern=retired_regex_pattern.negative_regex_pattern,
    )

    update_work_status_retired = retired_regex_hit | flag_records_for_retired_rules()

    # Not-working
    not_working_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=not_working_pattern.positive_regex_pattern,
        negative_regex_pattern=not_working_pattern.negative_regex_pattern,
    )

    update_work_status_not_working_v0 = not_working_regex_hit & flag_records_for_not_working_rules_v0()
    update_work_status_not_working_v1_a = not_working_regex_hit & flag_records_for_not_working_rules_v1_a()
    update_work_status_not_working_v1_b = not_working_regex_hit & flag_records_for_not_working_rules_v1_b()
    update_work_status_not_working_v2_a = not_working_regex_hit & flag_records_for_not_working_rules_v2_a()
    update_work_status_not_working_v2_b = not_working_regex_hit & flag_records_for_not_working_rules_v2_b()

    # School/Student
    school_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=at_school_pattern.positive_regex_pattern,
        negative_regex_pattern=at_school_pattern.negative_regex_pattern,
    )

    college_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=in_college_or_further_education_pattern.positive_regex_pattern,
        negative_regex_pattern=in_college_or_further_education_pattern.negative_regex_pattern,
    )

    university_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=at_university_pattern.positive_regex_pattern,
        negative_regex_pattern=at_university_pattern.negative_regex_pattern,
    )

    # Childcare
    childcare_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=childcare_pattern.positive_regex_pattern,
        negative_regex_pattern=childcare_pattern.negative_regex_pattern,
    )

    age_under_16 = F.col("age_at_visit") < F.lit(16)
    age_over_four = F.col("age_at_visit") > F.lit(4)

    update_work_status_student_v0 = (
        (school_regex_hit & flag_records_for_school_v2_rules())
        | (university_regex_hit & flag_records_for_uni_v0_rules())
        | (college_regex_hit & flag_records_for_college_v0_rules())
        | (age_over_four & age_under_16)
    )

    update_work_status_student_v1_a = (
        (school_regex_hit & flag_records_for_student_v1_rules())
        | (university_regex_hit & flag_records_for_student_v1_rules())
        | (college_regex_hit & flag_records_for_student_v1_rules())
        | (age_over_four & age_under_16)
    )

    update_work_status_student_v1_b = ~childcare_regex_hit & flag_records_for_childcare_v1_rules()  # type: ignore

    update_work_status_student_v1_c = (childcare_regex_hit & flag_records_for_childcare_v1_rules()) | (
        school_regex_hit & flag_records_for_childcare_v1_rules()
    )

    update_work_status_student_v2_a = (school_regex_hit & flag_records_for_school_v2_rules()) | (
        age_over_four & age_under_16
    )

    update_work_status_student_v2_b = college_regex_hit & flag_records_for_college_v2_rules()

    update_work_status_student_v2_c = university_regex_hit & flag_records_for_uni_v2_rules()

    update_work_status_student_v2_d = ~childcare_regex_hit & flag_records_for_childcare_v2_b_rules()  # type: ignore

    update_work_status_student_v2_e = (childcare_regex_hit & flag_records_for_childcare_v2_b_rules()) | (
        school_regex_hit & flag_records_for_childcare_v2_b_rules()
    )

    update_work_location_general = flag_records_for_work_location_null()

    # Please note the order of *_edited columns, these must come before the in-place updates

    # first start by taking a copy of the original work variables
    _df = (
        df.withColumn("work_location_original", F.col("work_location"))
        .withColumn("work_status_v0_original", F.col("work_status_v0"))
        .withColumn("work_status_v1_original", F.col("work_status_v1"))
        .withColumn("work_status_v2_original", F.col("work_status_v2"))
        .withColumn(
            "work_location",
            F.when(update_work_location, F.lit("Working from home")).otherwise(F.col("work_location")),
        )
        .withColumn(
            "work_status_v0",
            F.when(self_employed_regex_hit, F.lit("Self-employed")).otherwise(F.col("work_status_v0")),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_self_employed_v1_a, F.lit("Self-employed and currently working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_self_employed_v1_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(
                update_work_status_self_employed_v2_a,
                F.lit("Self-employed and currently working"),
            ).otherwise(F.col("work_status_v2")),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_self_employed_v2_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v0",
            F.when(update_work_status_student_v0, F.lit("Student")).otherwise(F.col("work_status_v0")),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_student_v1_a, F.lit("5y and older in full-time education")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_student_v1_b, F.lit("Child under 5y not attending child care")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_student_v1_c, F.lit("Child under 5y attending child care")).otherwise(
                F.col("work_status_v1")
            ),
        )
    )

    _df2 = spark_session.createDataFrame(_df.rdd, schema=_df.schema)  # breaks lineage

    _df3 = (
        _df2.withColumn(
            "work_status_v2",
            F.when(update_work_status_student_v2_a, F.lit("4-5y and older at school/home-school")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(
                update_work_status_student_v2_b, F.lit("Attending college or FE (including if temporarily absent)")
            ).otherwise(F.col("work_status_v2")),
        )
        .withColumn(
            "work_status_v2",
            F.when(
                update_work_status_student_v2_c, F.lit("Attending university (including if temporarily absent)")
            ).otherwise(F.col("work_status_v2")),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_student_v2_d, F.lit("Child under 4-5y not attending child care")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_student_v2_e, F.lit("Child under 4-5y attending child care")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v0",
            F.when(
                update_work_status_retired, F.lit("Not working (unemployed, retired, long-term sick etc.)")
            ).otherwise(F.col("work_status_v0")),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_retired, F.lit("Retired")).otherwise(F.col("work_status_v1")),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_retired, F.lit("Retired")).otherwise(F.col("work_status_v2")),
        )
        .withColumn(
            "work_status_v0",
            F.when(
                update_work_status_not_working_v0, F.lit("Not working (unemployed, retired, long-term sick etc.)")
            ).otherwise(F.col("work_status_v0")),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_not_working_v1_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_not_working_v1_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_not_working_v2_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_not_working_v2_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v0",
            F.when(update_work_status_furlough_v0, F.lit("Furloughed (temporarily not working)")).otherwise(
                F.col("work_status_v0")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_furlough_v1_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_furlough_v1_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_furlough_v2_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_furlough_v2_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_location",
            F.when(
                update_work_location_general,
                F.lit("Not applicable, not currently working"),
            ).otherwise(F.col("work_location")),
        )
    )

    if drop_original_variables:
        # replace original versions with their cleaned versions
        _df3 = _df3.drop(
            "work_location_original",
            "work_status_v0_original",
            "work_status_v1_original",
            "work_status_v2_original",
        )

    return _df3
