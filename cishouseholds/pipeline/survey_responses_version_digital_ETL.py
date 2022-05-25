import pyspark.sql.functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic


def digital_specific_transformations(df):
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 3)
    df = df.withColumn("face_covering_outside_of_home", F.lit(None).cast("string"))
    df = df.withColumn("visit_id", F.col("participant_completion_window_id"))
    df = df.withColumn("visit_datetime", F.col("digital_enrolment_invite_datetime"))
    df = df.withColumn("times_outside_shopping_or_socialising_last_7_days", F.lit(None))
    df = transform_survey_responses_generic(df)
    df = edit_to_sum_or_max_value(
        df=df,
        column_name_to_assign="times_outside_shopping_or_socialising_last_7_days",
        columns_to_sum=[
            "times_shopping_last_7_days",
            "times_socialise_last_7_days",
        ],
        max_value=7,
    )
    df = df.withColumn(
        "work_not_from_home_days_per_week",
        F.greatest("work_not_from_home_days_per_week", "education_in_person_days_per_week"),
    )
    return df
