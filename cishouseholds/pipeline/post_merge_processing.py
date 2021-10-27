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

    df = extract_from_table("transformed_survey_antibody_merge_data")

    if check_table_exists("imputed_value_lookup"):
        imputed_value_lookup_df = extract_from_table("imputed_value_lookup")
    else:
        imputed_value_lookup_df = None

    demographic_columns = ["white_group", "sex", "date_of_birth"]

    impute_key_demographics(df, imputed_value_lookup_df, demographic_columns)

    imputed_df = df.filter(F.sum(F.col(f"{column}_is_imputed") for column in demographic_columns) > 0)

    df_to_write = imputed_df.select(
        "participant_id",
        *chain([(column, f"{column}_is_imputed", f"{column}_imputation_method") for column in demographic_columns]),
    )

    update_table(df_to_write, "imputed_value_lookup")

    pass


def impute_key_demographics(df: DataFrame, imputed_value_lookup_df: DataFrame, demographic_columns: list):
    """Impute missing values for key variables that are required for weight calibration."""

    for column in demographic_columns:
        df = impute_and_flag(
            df,
            imputation_function=impute_by_ordered_fill_forward,
            reference_column=column,
            column_identity="participant_id",
            order_by_column="visit_datetime",
            order_type="asc",
        )
        df = impute_and_flag(
            df,
            imputation_function=impute_by_ordered_fill_forward,
            reference_column=column,
            column_identity="participant_id",
            order_by_column="visit_datetime",
            order_type="desc",
        )

    if imputed_value_lookup_df is not None:
        merge_previous_imputed_values(df, imputed_value_lookup_df, "participant_id")

    df = impute_and_flag(
        df,
        imputation_function=impute_by_mode,
        reference_column="white_group",
        group_by_column="ons_household_id",
    )
    # Todo: Add call to impute white_group by donor-based imputation

    df = impute_and_flag(
        df,
        imputation_function=impute_by_distribution,
        reference_column="sex",
        group_by_columns=["white_group", "gor9d"],
        first_imputation_value="Female",
        second_imputation_value="Male",
    )
    # Todo: Add call to impute date_of_birth using donor-based imputation

    return df
