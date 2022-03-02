from functools import reduce
from itertools import chain
from typing import List

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_multigeneration
from cishouseholds.edit import rename_column_names
from cishouseholds.impute import impute_and_flag
from cishouseholds.impute import impute_by_distribution
from cishouseholds.impute import impute_by_k_nearest_neighbours
from cishouseholds.impute import impute_by_mode
from cishouseholds.impute import impute_by_ordered_fill_forward
from cishouseholds.impute import knn_imputation_for_date
from cishouseholds.impute import merge_previous_imputed_values
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.input_variable_names import nims_column_name_map
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage

# from cishouseholds.pipeline.load import check_table_exists


@register_pipeline_stage("geography_and_imputation_logic")
def process_post_merge(
    imputed_antibody_swab_table: str,
    response_records_table: str,
    invalid_response_records_table: str,
    key_columns: List[str],
):
    df_with_imputed_values = extract_from_table(imputed_antibody_swab_table)
    df_with_imputed_values = merge_dependent_transform(df_with_imputed_values)

    imputation_columns = chain(
        *[(column, f"{column}_imputation_method", f"{column}_is_imputed") for column in key_columns]
    )
    response_level_records_df = df_with_imputed_values.drop(*imputation_columns)

    response_level_records_df, response_level_records_filtered_df = filter_response_records(
        response_level_records_df, "visit_datetime"
    )

    multigeneration_df = assign_multigeneration(
        df=response_level_records_df,
        column_name_to_assign="multigen",
        participant_id_column="participant_id",
        household_id_column="ons_household_id",
        visit_date_column="visit_date_string",
        date_of_birth_column="date_of_birth",
        country_column="country_name",
    )

    update_table(multigeneration_df, "multigeneration_table", mode_overide="overwrite")
    update_table(response_level_records_df, response_records_table, mode_overide="overwrite")
    update_table(response_level_records_filtered_df, invalid_response_records_table, mode_overide=None)


@register_pipeline_stage("impute_demographic_columns")
def impute_key_columns_stage(
    survey_responses_table: str,
    imputed_values_table: str,
    survey_responses_imputed_table: str,
    key_columns: List[str],
):

    imputed_value_lookup_df = extract_from_table(imputed_values_table)
    df = extract_from_table(survey_responses_table)

    key_columns_imputed_df = impute_key_columns(
        df, imputed_value_lookup_df, key_columns, get_config().get("imputation_log_directory", "./")
    )
    imputed_values_df = key_columns_imputed_df.filter(
        reduce(
            lambda col_1, col_2: col_1 | col_2,
            (F.col(f"{column}_imputation_method").isNotNull() for column in key_columns),
        )
    )

    lookup_columns = chain(*[(column, f"{column}_imputation_method") for column in key_columns])
    imputed_values = imputed_values_df.select(
        "participant_id",
        *lookup_columns,
    )
    df_with_imputed_values = df.drop(*key_columns).join(key_columns_imputed_df, on="participant_id", how="left")

    update_table(imputed_values, imputed_values_table)
    update_table(df_with_imputed_values, survey_responses_imputed_table, "overwrite")


def impute_key_columns(df: DataFrame, imputed_value_lookup_df: DataFrame, columns_to_fill: list, log_directory: str):
    """
    Impute missing values for key variables that are required for weight calibration.
    Most imputations require geographic data being joined onto the participant records.

    Returns a single record per participant.
    """
    unique_id_column = "participant_id"
    for column in columns_to_fill:
        df = impute_and_flag(
            df,
            imputation_function=impute_by_ordered_fill_forward,
            reference_column=column,
            column_identity=unique_id_column,
            order_by_column="visit_datetime",
            order_type="asc",
        )
        df = impute_and_flag(
            df,
            imputation_function=impute_by_ordered_fill_forward,
            reference_column=column,
            column_identity=unique_id_column,
            order_by_column="visit_datetime",
            order_type="desc",
        )
    deduplicated_df = df.dropDuplicates([unique_id_column] + columns_to_fill)

    if imputed_value_lookup_df is not None:
        deduplicated_df = merge_previous_imputed_values(deduplicated_df, imputed_value_lookup_df, unique_id_column)

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_mode,
        reference_column="white_group",
        group_by_column="ons_household_id",
    )

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_by_k_nearest_neighbours,
        reference_column="white_group",
        donor_group_columns=["cis_area"],
        donor_group_column_weights=[5000],
        log_file_path=log_directory,
    )

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_distribution,
        reference_column="sex",
        group_by_columns=["white_group", "gor9d"],
        first_imputation_value="Female",
        second_imputation_value="Male",
    )
    # TODO: replace by new function
    # deduplicated_df = impute_and_flag(
    #     deduplicated_df,
    #     impute_by_k_nearest_neighbours,
    #     reference_column="date_of_birth",
    #     donor_group_columns=["gor9d", "work_status_group", "dvhsize"],
    #     log_file_path=log_directory,
    # )
    deduplicated_df = knn_imputation_for_date(
        df=deduplicated_df,
        column_name_to_assign="date_of_birth",
        reference_column="date_of_birth",
        donor_group_columns=["gor9d", "work_status_group", "dvhsize"],
        log_file_path=log_directory,
    )

    return deduplicated_df.select(
        unique_id_column, *columns_to_fill, *[col for col in deduplicated_df.columns if "_imputation_method" in col]
    )


def merge_dependent_transform(df: DataFrame):
    """
    Transformations depending on the merged dataset or imputed columns.
    """
    return df


@register_pipeline_stage("join_vaccination_data")
def join_vaccination_data(**kwargs):
    """
    Join NIMS vaccination data onto participant level records and derive vaccination status using NIMS and CIS data.
    """
    participant_df = extract_from_table(kwargs["participant_records_table"])
    nims_df = extract_from_table(kwargs["nims_table"])
    nims_df = nims_transformations(nims_df)

    participant_df = participant_df.join(nims_df, on="participant_id", how="left")
    participant_df = derive_overall_vaccination(participant_df)

    update_table(participant_df, kwargs["vaccination_data_table"], mode_overide="overwrite")


@register_pipeline_stage("join_geographic_data")
def join_geographic_data(
    geographic_table: str, survey_responses_table: str, geographic_responses_table: str, id_column: str
):
    """
    Join weights file onto survey data by household id.
    """
    weights_df = extract_from_table(geographic_table)
    survey_responses_df = extract_from_table(survey_responses_table)
    geographic_survey_df = survey_responses_df.join(weights_df, on=id_column, how="left")
    update_table(geographic_survey_df, geographic_responses_table)


def nims_transformations(df: DataFrame) -> DataFrame:
    """Clean and transform NIMS data after reading from table."""
    df = rename_column_names(df, nims_column_name_map)
    df = assign_column_to_date_string(df, "nims_vaccine_dose_1_date", reference_column="nims_vaccine_dose_1_datetime")
    df = assign_column_to_date_string(df, "nims_vaccine_dose_2_date", reference_column="nims_vaccine_dose_2_datetime")

    # TODO: Derive nims_linkage_status, nims_vaccine_classification, nims_vaccine_dose_1_time, nims_vaccine_dose_2_time
    return df


def derive_overall_vaccination(df: DataFrame) -> DataFrame:
    """Derive overall vaccination status from NIMS and CIS data."""
    return df


def filter_response_records(df: DataFrame, visit_date: str):
    """Creating column for file datetime and filter out dates after file date"""
    df = df.withColumn("file_date", F.regexp_extract(F.col("survey_response_source_file"), r"\d{8}(?=.csv)", 0))
    df = df.withColumn("file_date", F.to_timestamp(F.col("file_date"), format="yyyyMMdd"))
    df = df.withColumn(
        "filter_response_flag",
        F.when(
            (
                (F.col("file_date") < F.col(visit_date))
                & F.col("swab_sample_barcode").isNull()
                & F.col("blood_sample_barcode").isNull()
            ),
            1,
        ).otherwise(None),
    )
    df_flagged = df.where(F.col("filter_response_flag") == 1)
    df = df.where(F.col("filter_response_flag").isNull())

    return df.drop("filter_response_flag"), df_flagged.drop("filter_response_flag")
