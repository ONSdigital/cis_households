# flake8: noqa
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_column_from_mapped_list_key
from cishouseholds.derive import assign_column_given_proportion
from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_date_difference
from cishouseholds.derive import assign_ethnicity_white
from cishouseholds.derive import assign_ever_had_long_term_health_condition_or_disabled
from cishouseholds.derive import assign_fake_id
from cishouseholds.derive import assign_first_visit
from cishouseholds.derive import assign_household_participant_count
from cishouseholds.derive import assign_household_under_2_count
from cishouseholds.derive import assign_last_non_null_value_from_col_list
from cishouseholds.derive import assign_last_visit
from cishouseholds.derive import assign_named_buckets
from cishouseholds.derive import assign_visit_order
from cishouseholds.derive import assign_work_status_group
from cishouseholds.derive import contact_known_or_suspected_covid_type
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import convert_null_if_not_in_list
from cishouseholds.edit import correct_date_ranges_union_dependent
from cishouseholds.edit import remove_incorrect_dates
from cishouseholds.edit import replace_sample_barcode
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_face_covering_outside_of_home
from cishouseholds.edit import update_person_count_from_ages
from cishouseholds.edit import update_to_value_if_any_not_null
from cishouseholds.expressions import any_column_equal_value
from cishouseholds.expressions import sum_within_row
from cishouseholds.impute import fill_backwards_overriding_not_nulls
from cishouseholds.impute import fill_backwards_work_status_v2
from cishouseholds.impute import fill_forward_from_last_change
from cishouseholds.impute import fill_forward_only_to_nulls
from cishouseholds.impute import impute_and_flag
from cishouseholds.impute import impute_by_distribution
from cishouseholds.impute import impute_by_k_nearest_neighbours
from cishouseholds.impute import impute_by_mode
from cishouseholds.impute import impute_date_by_k_nearest_neighbours
from cishouseholds.impute import merge_previous_imputed_values
from cishouseholds.pipeline.mapping import date_cols_min_date_dict
from cishouseholds.pipeline.timestamp_map import cis_digital_datetime_map


def post_union_transformations(df: DataFrame) -> DataFrame:
    """apply all transformations that occur immediately after union in order."""
    df = create_formatted_datetime_string_columns(df).custom_checkpoint()
    df = fill_forwards(df).custom_checkpoint()
    df = union_dependent_cleaning(df).custom_checkpoint()
    df = union_dependent_derivations(df).custom_checkpoint()
    return df


def fill_forwards(df: DataFrame) -> DataFrame:
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
    # df = update_column_if_ref_in_list(
    #     df=df,
    #     column_name_to_update="work_location",
    #     old_value=None,
    #     new_value="Not applicable, not currently working",
    #     reference_column="work_status_v0",
    #     check_list=[
    #         "Furloughed (temporarily not working)",
    #         "Not working (unemployed, retired, long-term sick etc.)",
    #         "Student",
    #     ],
    # )

    df = update_to_value_if_any_not_null(
        df=df,
        column_name_to_assign="been_outside_uk",
        true_false_values=["Yes", "No"],
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


def union_dependent_cleaning(df) -> DataFrame:
    col_val_map = {
        "participant_withdrawal_reason": {
            "Bad experience with tester / survey": "Bad experience with interviewer/survey",
            "Swab / blood process too distressing": "Swab/blood process too distressing",
            "Swab / blood process to distressing": "Swab/blood process too distressing",
            "Do NOT Reinstate": "Do not reinstate",
        },
        "work_health_care_area": {
            "Secondary care for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services?": "Other",
            "Primary care - for example in a GP or dentist": "Primary",
            "Yes, in primary care, e.g. GP, dentist": "Primary",
            "Secondary care - for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services": "Other",  # noqa: E501
        },
    }
    date_cols_to_correct = [
        col
        for col in [
            "last_covid_contact_date",
            "last_suspected_covid_contact_date",
            "think_had_covid_onset_date",
            "think_have_covid_onset_date",
            "been_outside_uk_last_return_date",
            "other_covid_infection_test_first_positive_date",
            "other_covid_infection_test_last_negative_date",
            "other_antibody_test_first_positive_date",
            "other_antibody_test_last_negative_date",
        ]
        if col in df.columns
    ]
    df = correct_date_ranges_union_dependent(df, date_cols_to_correct, "participant_id", "visit_datetime", "visit_id")
    df = remove_incorrect_dates(df, date_cols_to_correct, "visit_datetime", "2019-08-01", date_cols_min_date_dict)

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


def create_ever_variable_columns(df: DataFrame) -> DataFrame:
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_work_person_facing_or_social_care",
        groupby_column="participant_id",
        reference_columns=["work_social_care"],
        count_if=["Yes, care/residential home, resident-facing", "Yes, other social care, resident-facing", "Yes"],
        true_false_values=["Yes", "No"],
    )
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_care_home_worker",
        groupby_column="participant_id",
        reference_columns=["work_social_care", "work_nursing_or_residential_care_home"],
        count_if=["Yes", "Yes, care/residential home, resident-facing"],
        true_false_values=["Yes", "No"],
    )
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_had_long_term_health_condition",
        groupby_column="participant_id",
        reference_columns=["illness_lasting_over_12_months"],
        count_if=["Yes"],
        true_false_values=["Yes", "No"],
    )
    df = assign_ever_had_long_term_health_condition_or_disabled(
        df=df,
        column_name_to_assign="ever_had_long_term_health_condition_or_disabled",
        health_conditions_column="illness_lasting_over_12_months",
        condition_impact_column="illness_reduces_activity_or_ability",
    )
    return df


def create_formatted_datetime_string_columns(df) -> DataFrame:
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


def union_dependent_derivations(df) -> DataFrame:
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
    if "survey_completion_status" in df.columns:
        df = df.withColumn(
            "participant_visit_status", F.coalesce(F.col("participant_visit_status"), F.col("survey_completion_status"))
        )

    df = replace_sample_barcode(df=df)

    df = create_ever_variable_columns(df)

    df = assign_first_visit(
        df=df,
        column_name_to_assign="household_first_visit_datetime",
        id_column="ons_household_id",
        visit_date_column="visit_datetime",
    )
    df = assign_last_visit(
        df=df,
        column_name_to_assign="last_attended_visit_datetime",
        id_column="ons_household_id",
        visit_status_column="participant_visit_status",
        visit_date_column="visit_datetime",
    )
    df = assign_date_difference(
        df=df,
        column_name_to_assign="days_since_enrolment",
        start_reference_column="household_first_visit_datetime",
        end_reference_column="visit_datetime",
    )
    df = assign_date_difference(
        df=df,
        column_name_to_assign="household_weeks_since_survey_enrolment",
        start_reference_column="survey start",
        end_reference_column="visit_datetime",
        format="weeks",
    )
    df = assign_named_buckets(
        df,
        reference_column="days_since_enrolment",
        column_name_to_assign="visit_number",
        map={**{0: 1, 14: 2, 21: 3, 28: 4}, **{i * 28: (i + 3) for i in range(2, 200)}},
    )
    df = derive_people_in_household_count(df)
    df = update_column_values_from_map(
        df=df,
        column="smokes_nothing_now",
        map={"Yes": "No", "No": "Yes"},
        condition_column="currently_smokes_or_vapes",
    )
    return df


def derive_contact_any_covid_covid_variables(df: DataFrame) -> DataFrame:
    """
    Derive variables related to combination of know and suspected covid data columns.
    """
    df = df.withColumn(
        "contact_known_or_suspected_covid",
        F.when(
            any_column_equal_value(
                ["contact_suspected_positive_covid_last_28_days", "contact_known_positive_covid_last_28_days"], "Yes"
            ),
            "Yes",
        ).otherwise("No"),
    )

    df = assign_last_non_null_value_from_col_list(
        df=df,
        column_name_to_assign="contact_known_or_suspected_covid_latest_date",
        column_list=["last_covid_contact_date", "last_suspected_covid_contact_date"],
    )

    df = contact_known_or_suspected_covid_type(
        df=df,
        contact_known_covid_type_column="last_covid_contact_type",
        contact_suspect_covid_type_column="last_suspected_covid_contact_type",
        contact_any_covid_type_column="contact_known_or_suspected_covid",
        contact_any_covid_date_column="contact_known_or_suspected_covid_latest_date",
        contact_known_covid_date_column="last_covid_contact_date",
        contact_suspect_covid_date_column="last_suspected_covid_contact_date",
    )

    df = assign_date_difference(
        df,
        "contact_known_or_suspected_covid_days_since",
        "contact_known_or_suspected_covid_latest_date",
        "visit_datetime",
    )

    return df


def impute_key_columns(df: DataFrame, imputed_value_lookup_df: DataFrame, log_directory: str) -> DataFrame:
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


def fix_timestamps(df: DataFrame) -> DataFrame:
    """
    Fix any issues with dates saved in timestamp format drifting ahead by n hours.
    """
    date_cols = [c for c in df.columns if "date" in c and "datetime" not in c]
    d_types_list = [list(d) for d in df.select(*date_cols).dtypes]
    d_types = {d[0]: d[1] for d in d_types_list}
    for col in date_cols:
        if d_types[col] == "timestamp":
            df = df.withColumn(col, F.date_format(F.col(col), "yyyy-MM-dd"))
    return df


def get_differences(
    base_df: DataFrame, compare_df: DataFrame, unique_id_column: str, diff_sample_size: int = 10
) -> DataFrame:
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
