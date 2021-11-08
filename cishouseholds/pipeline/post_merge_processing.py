from functools import reduce
from itertools import chain

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from cishouseholds.impute import impute_and_flag
from cishouseholds.impute import impute_by_distribution
from cishouseholds.impute import impute_by_mode
from cishouseholds.impute import impute_by_ordered_fill_forward
from cishouseholds.impute import merge_previous_imputed_values
from cishouseholds.pipeline.load import check_table_exists
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage


@register_pipeline_stage("process_post_merge")
def process_post_merge():

    df = extract_from_table("merged_responses_antibody_swab_data")

    if check_table_exists("imputed_value_lookup"):
        imputed_value_lookup_df = extract_from_table("imputed_value_lookup")
    else:
        imputed_value_lookup_df = None

    # TODO: Need to join geographies from household level table before imputing
    if "gor9d" not in df.columns:
        df = df.withColumn("gor9d", F.lit("A"))
    # TODO: Remove once white group derived on survey responses
    if "white_group" not in df.columns:
        df = df.withColumn("white_group", F.lit("white"))

    key_columns = ["white_group", "sex", "date_of_birth"]
    key_columns_imputed_df = impute_key_columns(df, imputed_value_lookup_df, key_columns)

    imputed_values_df = key_columns_imputed_df.filter(
        reduce(
            lambda col_1, col_2: col_1 | col_2,
            (F.col(f"{column}_imputation_method").isNotNull() for column in key_columns),
        )
    )
    update_table(key_columns_imputed_df, "participant_level_key_records", mode_overide="overwrite")

    lookup_columns = chain(*[(column, f"{column}_imputation_method") for column in key_columns])
    imputed_values = imputed_values_df.select(
        "participant_id",
        *lookup_columns,
    )
    update_table(imputed_values, "imputed_value_lookup")

    df_with_imputed_values = df.drop(*key_columns).join(key_columns_imputed_df, on="participant_id", how="left")
    df_with_imputed_values = merge_dependent_transform(df_with_imputed_values)

    imputation_columns = chain(
        *[(column, f"{column}_imputation_method", f"{column}_is_imputed") for column in key_columns]
    )
    response_level_records_df = df_with_imputed_values.drop(*imputation_columns)
    update_table(response_level_records_df, "response_level_records", mode_overide="overwrite")


def impute_key_columns(df: DataFrame, imputed_value_lookup_df: DataFrame, columns_to_fill: list):
    """
    Impute missing values for key variables that are required for weight calibration.
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
    # TODO: Add call to impute white_group by donor-based imputation

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_distribution,
        reference_column="sex",
        group_by_columns=["white_group", "gor9d"],
        first_imputation_value="Female",
        second_imputation_value="Male",
    )  # Relies on sample data being joined on

    # TODO: Add call to impute date_of_birth using donor-based imputation

    return deduplicated_df.select(
        unique_id_column, *columns_to_fill, *[col for col in deduplicated_df.columns if "_imputation_method" in col]
    )


def merge_dependent_transform(df: DataFrame):
    """
    Transformations depending on the merged dataset or imputed columns.
    """
    return df
