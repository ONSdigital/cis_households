from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.edit import rename_column_names
from cishouseholds.impute import impute_and_flag
from cishouseholds.impute import impute_by_distribution
from cishouseholds.impute import impute_by_k_nearest_neighbours
from cishouseholds.impute import impute_by_mode
from cishouseholds.impute import impute_by_ordered_fill_forward
from cishouseholds.impute import merge_previous_imputed_values
from cishouseholds.pipeline.input_variable_names import column_name_maps


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
        reference_column="ethnicity_white",
        group_by_column="ons_household_id",
    )

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_by_k_nearest_neighbours,
        reference_column="ethnicity_white",
        donor_group_columns=["cis_area_code_20"],
        donor_group_column_weights=[5000],
        log_file_path=log_directory,
    )

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_distribution,
        reference_column="sex",
        group_by_columns=["ethnicity_white", "region_code"],
        first_imputation_value="Female",
        second_imputation_value="Male",
    )

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_by_k_nearest_neighbours,
        reference_column="date_of_birth",
        donor_group_columns=["region_code", "people_in_household_count_group", "work_status_group"],
        log_file_path=log_directory,
    )

    return deduplicated_df.select(
        unique_id_column,
        *columns_to_fill,
        *[col for col in deduplicated_df.columns if col.endswith("_imputation_method")],
        *[col for col in deduplicated_df.columns if col.endswith("_is_imputed")]
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
