from functools import reduce
from itertools import chain

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.edit import rename_column_names
from cishouseholds.impute import impute_and_flag
from cishouseholds.impute import impute_by_distribution
from cishouseholds.impute import impute_by_k_nearest_neighbours
from cishouseholds.impute import impute_by_mode
from cishouseholds.impute import impute_by_ordered_fill_forward
from cishouseholds.impute import merge_previous_imputed_values
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.input_variable_names import nims_column_name_map
from cishouseholds.pipeline.load import check_table_exists
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage


@register_pipeline_stage("process_post_merge")
def process_post_merge(**kwargs):
    df = extract_from_table(kwargs["merged_antibody_swab_table"])

    if check_table_exists(kwargs["imputed_values_table"]):
        imputed_value_lookup_df = extract_from_table(kwargs["imputed_values_table"])
    else:
        imputed_value_lookup_df = None

    # TODO: Need to join geographies from household level table before imputing
    for col in ["gor9d", "work_status_group", "dvhsize", "cis_area"]:
        if col not in df.columns:
            df = df.withColumn(col, F.lit("A"))
    # TODO: Remove once white group derived on survey responses
    if "white_group" not in df.columns:
        df = df.withColumn("white_group", F.lit("white"))

    key_columns = ["white_group", "sex", "date_of_birth"]
    key_columns_imputed_df = impute_key_columns(
        df, imputed_value_lookup_df, key_columns, get_config().get("imputation_log_directory", "./")
    )

    imputed_values_df = key_columns_imputed_df.filter(
        reduce(
            lambda col_1, col_2: col_1 | col_2,
            (F.col(f"{column}_imputation_method").isNotNull() for column in key_columns),
        )
    )
    update_table(key_columns_imputed_df, kwargs["participant_records_table"], mode_overide="overwrite")

    lookup_columns = chain(*[(column, f"{column}_imputation_method") for column in key_columns])
    imputed_values = imputed_values_df.select(
        "participant_id",
        *lookup_columns,
    )
    update_table(imputed_values, kwargs["imputed_values_table"])

    df_with_imputed_values = df.drop(*key_columns).join(key_columns_imputed_df, on="participant_id", how="left")
    df_with_imputed_values = merge_dependent_transform(df_with_imputed_values)

    imputation_columns = chain(
        *[(column, f"{column}_imputation_method", f"{column}_is_imputed") for column in key_columns]
    )
    response_level_records_df = df_with_imputed_values.drop(*imputation_columns)

    response_level_records_df, response_level_records_filtered_df = filter_response_records(
        response_level_records_df, "visit_datetime"
    )

    update_table(response_level_records_df, kwargs["response_records_table"], mode_overide="overwrite")
    update_table(response_level_records_filtered_df, kwargs["invalid_response_records_table"], mode_overide=None)


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

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_by_k_nearest_neighbours,
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
