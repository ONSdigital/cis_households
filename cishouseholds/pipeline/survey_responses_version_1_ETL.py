from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_uniform_value


def transform_survey_responses_version_1_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 1 survey deltas.
    """
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 1)

    return df
