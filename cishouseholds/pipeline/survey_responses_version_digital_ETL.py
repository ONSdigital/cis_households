import pyspark.sql.functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic


def digital_specific_transformations(df):
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 3)
    df = df.withColumn("face_covering_outside_of_home", F.lit(None).cast("string"))
    df = df.withColumn("visit_id", F.col("participant_completion_window_id"))
    df = df.withColumn("visit_datetime", F.col("digital_enrolment_invite_datetime"))
    df = transform_survey_responses_generic(df)
    column_list = ["work_status_digital", "work_status_employment", "work_status_unemployment", "work_status_education"]
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v2",
        [
            [1, [1, 1, None, None]],
            [2, [1, 2, None, None]],
            [3, [2, 1, None, None]],
            [4, [2, 2, None, None]],
            [5, [3, None, 1, None]],
            [6, [3, None, 2, None]],
            [7, [3, None, 3, None]],
            [8, [4, None, None, 1]],
            [9, [4, None, None, 2]],
            [10, [4, None, None, [3, 4]]],
            [11, [4, None, None, 5]],
            [12, [4, None, None, 6]],
        ],
        column_list,
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v0",
        [
            [1, [1, 1, None, None]],
            [4, [1, 2, None, None]],
            [1, [2, None, 1, None]],
            [2, [2, None, 2, None]],
            [2, [None, None, None]],
            [4, [2, None, None, None]],
            [4, [3, None, 1, None]],
            [4, [3, None, 2, None]],
            [4, [3, None, 3, None]],
            [4, [3, None, None, None]],
            [5, [4, None, None, 1]],
            [5, [4, None, None, 2]],
            [5, [4, None, None, 3]],
            [5, [4, None, None, 4]],
            [5, [4, None, None, 5]],
        ],
        column_list,
    )
    return df
