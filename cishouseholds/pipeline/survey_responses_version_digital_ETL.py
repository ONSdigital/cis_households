import pyspark.sql.functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.edit import clean_barcode_simple
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic


def digital_specific_transformations(df):
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 3)
    df = df.withColumn("face_covering_outside_of_home", F.lit(None).cast("string"))
    df = df.withColumn("visit_id", F.col("participant_completion_window_id"))
    df = df.withColumn("visit_datetime", F.col("digital_enrolment_invite_datetime"))
    df = df.withColumn("times_outside_shopping_or_socialising_last_7_days", F.lit(None))
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
    df = clean_barcode_simple(df, "swab_sample_barcode_user_entered")
    df = clean_barcode_simple(df, "blood_sample_barcode_user_entered")
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
