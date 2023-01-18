# flake8: noqa
from datetime import datetime
from functools import reduce
from operator import and_
from operator import or_
from typing import List

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_age_at_date
from cishouseholds.derive import assign_any_symptoms_around_visit
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
from cishouseholds.derive import assign_last_non_null_value_from_col_list
from cishouseholds.derive import assign_last_visit
from cishouseholds.derive import assign_named_buckets
from cishouseholds.derive import assign_outward_postcode
from cishouseholds.derive import assign_raw_copies
from cishouseholds.derive import assign_regex_from_map
from cishouseholds.derive import assign_regex_from_map_additional_rules
from cishouseholds.derive import assign_regex_match_result
from cishouseholds.derive import assign_school_year_september_start
from cishouseholds.derive import assign_taken_column
from cishouseholds.derive import assign_true_if_any
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.derive import assign_visit_order
from cishouseholds.derive import assign_work_health_care
from cishouseholds.derive import assign_work_social_column
from cishouseholds.derive import assign_work_status_group
from cishouseholds.derive import clean_postcode
from cishouseholds.derive import concat_fields_if_true
from cishouseholds.derive import contact_known_or_suspected_covid_type
from cishouseholds.derive import count_value_occurrences_in_column_subset_row_wise
from cishouseholds.derive import derive_had_symptom_last_7days_from_digital
from cishouseholds.derive import derive_household_been_columns
from cishouseholds.derive import flag_records_for_childcare_v0_rules
from cishouseholds.derive import flag_records_for_childcare_v1_rules
from cishouseholds.derive import flag_records_for_childcare_v2_b_rules
from cishouseholds.derive import flag_records_for_college_v0_rules
from cishouseholds.derive import flag_records_for_college_v1_rules
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
from cishouseholds.derive import flag_records_for_self_employed_rules_v0
from cishouseholds.derive import flag_records_for_self_employed_rules_v1_a
from cishouseholds.derive import flag_records_for_self_employed_rules_v1_b
from cishouseholds.derive import flag_records_for_self_employed_rules_v2_a
from cishouseholds.derive import flag_records_for_self_employed_rules_v2_b
from cishouseholds.derive import flag_records_for_uni_v0_rules
from cishouseholds.derive import flag_records_for_uni_v1_rules
from cishouseholds.derive import flag_records_for_uni_v2_rules
from cishouseholds.derive import flag_records_for_work_from_home_rules
from cishouseholds.derive import flag_records_for_work_location_null
from cishouseholds.derive import flag_records_for_work_location_student
from cishouseholds.derive import map_options_to_bool_columns
from cishouseholds.derive import regex_match_result
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import assign_from_map
from cishouseholds.edit import clean_barcode
from cishouseholds.edit import clean_barcode_simple
from cishouseholds.edit import clean_job_description_string
from cishouseholds.edit import clean_within_range
from cishouseholds.edit import conditionally_replace_columns
from cishouseholds.edit import conditionally_set_column_values
from cishouseholds.edit import convert_null_if_not_in_list
from cishouseholds.edit import correct_date_ranges
from cishouseholds.edit import correct_date_ranges_union_dependent
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.edit import format_string_upper_and_clean
from cishouseholds.edit import fuzzy_update
from cishouseholds.edit import map_column_values_to_null
from cishouseholds.edit import normalise_think_had_covid_columns
from cishouseholds.edit import remove_incorrect_dates
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import replace_sample_barcode
from cishouseholds.edit import survey_edit_auto_complete
from cishouseholds.edit import update_column_if_ref_in_list
from cishouseholds.edit import update_column_in_time_window
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_face_covering_outside_of_home
from cishouseholds.edit import update_from_lookup_df
from cishouseholds.edit import update_person_count_from_ages
from cishouseholds.edit import update_strings_to_sentence_case
from cishouseholds.edit import update_think_have_covid_symptom_any
from cishouseholds.edit import update_to_value_if_any_not_null
from cishouseholds.edit import update_value_if_multiple_and_ref_in_list
from cishouseholds.edit import update_work_facing_now_column
from cishouseholds.edit import update_work_main_job_changed
from cishouseholds.expressions import any_column_equal_value
from cishouseholds.expressions import any_column_null
from cishouseholds.expressions import array_contains_any
from cishouseholds.expressions import first_sorted_val_row_wise
from cishouseholds.expressions import last_sorted_val_row_wise
from cishouseholds.expressions import sum_within_row
from cishouseholds.impute import fill_backwards_overriding_not_nulls
from cishouseholds.impute import fill_backwards_work_status_v2
from cishouseholds.impute import fill_forward_event
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
from cishouseholds.merge import null_safe_join
from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.generate_outputs import generate_sample
from cishouseholds.pipeline.input_file_processing import extract_lookup_csv
from cishouseholds.pipeline.mapping import column_name_maps
from cishouseholds.pipeline.timestamp_map import cis_digital_datetime_map
from cishouseholds.pipeline.translate import backup_and_replace_translation_lookup_df
from cishouseholds.pipeline.translate import export_responses_to_be_translated_to_translation_directory
from cishouseholds.pipeline.translate import get_new_translations_from_completed_translations_directory
from cishouseholds.pipeline.translate import get_welsh_responses_to_be_translated
from cishouseholds.pipeline.translate import translate_welsh_fixed_text_responses_digital
from cishouseholds.pipeline.translate import translate_welsh_free_text_responses_digital
from cishouseholds.pipeline.validation_schema import validation_schemas  # noqa: F401
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.regex.healthcare_regex import healthcare_classification
from cishouseholds.regex.healthcare_regex import patient_facing_classification
from cishouseholds.regex.healthcare_regex import patient_facing_pattern
from cishouseholds.regex.healthcare_regex import priority_map
from cishouseholds.regex.healthcare_regex import roles_map
from cishouseholds.regex.healthcare_regex import social_care_classification
from cishouseholds.regex.regex_patterns import at_school_pattern
from cishouseholds.regex.regex_patterns import at_university_pattern
from cishouseholds.regex.regex_patterns import childcare_pattern
from cishouseholds.regex.regex_patterns import furloughed_pattern
from cishouseholds.regex.regex_patterns import in_college_or_further_education_pattern
from cishouseholds.regex.regex_patterns import not_working_pattern
from cishouseholds.regex.regex_patterns import retired_regex_pattern
from cishouseholds.regex.regex_patterns import self_employed_regex
from cishouseholds.regex.regex_patterns import work_from_home_pattern
from cishouseholds.regex.vaccine_regex import vaccine_regex_map
from cishouseholds.regex.vaccine_regex import vaccine_regex_priority_map
from cishouseholds.validate_class import SparkValidate


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


def flag_records_to_reclassify(df: DataFrame) -> DataFrame:
    """
    Adds various flags to indicate which rules were triggered for a given record.
    TODO: Don't use this function - it is not up to date with derive module
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
    # df = df.withColumn("student_rules_v0", flag_records_for_student_v0_rules())

    # df = df.withColumn("student_rules_v1", flag_records_for_student_v1_rules())

    df = df.withColumn("school_rules_v2", flag_records_for_school_v2_rules())

    # University rules
    df = df.withColumn("uni_rules_v2", flag_records_for_uni_v2_rules())

    df = df.withColumn("college_rules_v2", flag_records_for_college_v2_rules())

    return df


def get_differences(base_df: DataFrame, compare_df: DataFrame, unique_id_column: str, diff_sample_size: int = 10):
    window = Window.partitionBy("column_name").orderBy("column_name")
    cols_to_check = [col for col in base_df.columns if col in compare_df.columns and col != unique_id_column]

    for col in cols_to_check:
        base_df = base_df.withColumnRenamed(col, f"{col}_ref")

    df = base_df.join(compare_df, on=unique_id_column, how="left")

    diffs_df = df.select(
        [
            F.when(F.col(col).eqNullSafe(F.col(f"{col}_ref")), None).otherwise(F.col(unique_id_column)).alias(col)
            for col in cols_to_check
        ]
    )
    diffs_df = diffs_df.select(
        F.explode(
            F.array(
                [
                    F.struct(F.lit(col).alias("column_name"), F.col(col).alias(unique_id_column))
                    for col in diffs_df.columns
                ]
            )
        ).alias("kvs")
    )
    diffs_df = (
        diffs_df.select("kvs.column_name", f"kvs.{unique_id_column}")
        .filter(F.col(unique_id_column).isNotNull())
        .withColumn("ROW", F.row_number().over(window))
        .filter(F.col("ROW") < diff_sample_size)
    ).drop("ROW")

    counts_df = df.select(
        *[
            F.sum(F.when(F.col(c).eqNullSafe(F.col(f"{c}_ref")), 0).otherwise(1)).alias(c).cast("integer")
            for c in cols_to_check
        ],
        *[
            F.sum(F.when((~F.col(c).eqNullSafe(F.col(f"{c}_ref"))) & (F.col(f"{c}_ref").isNotNull()), 1).otherwise(0))
            .alias(f"{c}_non_improved")
            .cast("integer")
            for c in cols_to_check
        ],
    )
    counts_df = counts_df.select(
        F.explode(
            F.array(
                [
                    F.struct(
                        F.lit(col).alias("column_name"),
                        F.col(col).alias("difference_count"),
                        F.col(f"{col}_non_improved").alias("difference_count_non_improved"),
                    )
                    for col in [c for c in counts_df.columns if not c.endswith("_non_improved")]
                ]
            )
        ).alias("kvs")
    )
    counts_df = counts_df.select("kvs.column_name", "kvs.difference_count", "kvs.difference_count_non_improved")
    return counts_df, diffs_df
