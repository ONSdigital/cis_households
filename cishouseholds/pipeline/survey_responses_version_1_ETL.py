from pyspark.sql import DataFrame

from cishouseholds.edit import update_symptoms_last_7_days_any


def transform_survey_responses_version_1_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 1 survey deltas.
    """
    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="any_symptoms_last_7_days_or_now",
    #     reference_columns=["symptoms_last_7_days_any", "think_have_covid_symptoms_now"],
    #     true_false_values=["Yes", "No"],
    # )
    df = update_symptoms_last_7_days_any(
        df=df,
        column_name_to_update="symptoms_last_7_days_any",
        count_reference_column="symptoms_last_7_days_symptom_count",
    )
    return df
