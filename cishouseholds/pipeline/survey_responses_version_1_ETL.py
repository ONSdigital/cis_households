from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.edit import update_column_values_from_map


def transform_survey_responses_version_1_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 1 survey deltas.
    """
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 1)
    df = update_column_values_from_map(
        df=df,
        column="household_been_hospital_last_28_days",
        map={"No, someone else in my household has": "No I haven’t, but someone else in my household has"},
    )
    df = update_column_values_from_map(
        df=df,
        column="household_been_care_home_last_28_days",
        map={"No, someone else in my household has": "No I haven’t, but someone else in my household has"},
    )

    return df
