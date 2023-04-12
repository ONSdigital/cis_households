import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_survey_completed_status


@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, "true", None, "Partially Completed"),  # Flushed
            (2, "false", None, "Not Completed"),  # Not completed not flushed
            (3, "true", "2023-03-01 11:30:21", "Partially Completed",),  # Fushed but has datetime - not expected scenario
            (4, None, None, None),  # no information
            (5, "false", "2023-04-01 12:30:21", "Completed"),  # Not flushed, datetime present
        ],
        # fmt: on
        schema="id int, flushed string, survey_completed_dt string, status string",
    )


def test_assign_survey_completed_status(spark_session, expected_df):
    expected_df = expected_df.withColumn("survey_completed_dt", F.to_timestamp(F.col("survey_completed_dt")))

    input_df = expected_df.drop(F.col("status"))

    actual_df = assign_survey_completed_status(input_df, "status", "survey_completed_dt", "flushed")

    assert_df_equality(actual_df, expected_df)
