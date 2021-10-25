from cishouseholds.impute import impute_and_flag
from cishouseholds.impute import impute_by_distribution
from cishouseholds.impute import impute_by_mode
from cishouseholds.impute import impute_by_ordered_fill_forward
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage


@register_pipeline_stage("process_post_merge")
def process_post_merge():
    # Todo: Merge on previously imputed values
    pass


def impute_key_demographics(df):
    """Impute missing values for key variables that are required for weight calibration."""

    for demographic_column in ["white", "sex", "date_of_birth"]:
        df = impute_and_flag(
            df,
            imputation_function=impute_by_ordered_fill_forward,
            reference_column=demographic_column,
            column_identity="participant_id",
            order_by_column="visit_datetime",
        )

    df = impute_and_flag(
        df,
        imputation_function=impute_by_mode,
        reference_column="white",
        group_by_column="ons_household_id",
    )
    # Todo: Add call to impute white by donor-based imputation

    df = impute_and_flag(
        df,
        imputation_function=impute_by_distribution,
        reference_column="sex",
        group_by_columns=["white", "gor9d"],
        first_imputation_value="Female",
        second_imputation_value="Male",
    )
    # Todo: Add call to impute date_of_birth using donor-based imputation

    return df
