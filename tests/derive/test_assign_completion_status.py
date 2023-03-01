from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_completion_status


def test_assign_completion_status(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (
                1,
                "2023-01-01",
                "2023-01-14",
                "2023-01-05",
                "Continue",
                None,
                None,
                "2023-01-15",
                "Submitted",
            ),  # submitted q only
            (
                2,
                "2023-01-01",
                "2023-01-14",
                "2023-01-05",
                "Continue",
                "Continue",
                "FALSE",
                "2023-01-15",
                "Submitted",
            ),  # submitted bio and q
            (
                5,
                "2023-01-01",
                "2023-01-14",
                None,
                "Continue",
                "Continue",
                "TRUE",
                "2023-01-15",
                "Completed",
            ),  # flushed and file date after end date
            (
                3,
                "2022-12-01",
                "2022-12-14",
                None,
                None,
                None,
                "TRUE",
                "2022-12-10",
                "In progress",
            ),  # not started, flushed
            (
                4,
                "2023-01-01",
                "2023-01-14",
                None,
                None,
                None,
                "FALSE",
                "2023-01-13",
                "New",
            ),  # window open, not flushed
        ],
        schema="""
        participant_id integer,
        participant_completion_window_start_date string,
        participant_completion_window_end_date string,
        survey_completed_datetime string,
        end_screen_questionnaire string,
        end_screen_sample string,
        survey_completion_status_flushed string,
        file_date string,
        derived_completion_status string
        """,
    )
    for col in [
        "participant_completion_window_start_date",
        "participant_completion_window_end_date",
        "survey_completed_datetime",
        "file_date",
    ]:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col, format="yyyy-MM-dd"))

    output_df = assign_completion_status(
        df=expected_df.drop("derived_completion_status"), column_name_to_assign="derived_completion_status"
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True)
