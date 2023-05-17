import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_survey_completed_status


@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, True, None, "Yes", "Yes", "Partially Completed"),  # Flushed
            (2, False, None, "Yes", "Yes", "Not Completed"),  # Not completed not flushed
            (3, True, "2023-03-01 11:30:21", "Yes", "Yes", "Partially Completed",),  # Fushed but has datetime - not expected scenario
            (4, None, None, None, None, None),  # no information
            (5, False, "2023-04-01 12:30:21", "Yes", "Yes", "Completed"),  # Not flushed, datetime present
            (6, None, "2023-04-01 12:30:21","No", "Yes", "Completed" ) #filling in on behalf of someone
            (7, None, "2023-04-05 12:30:21","No", "No", "Non-response" ) #Not filling in on behalf of someone and not that person
            (8, False, None,"No", "No", "Non-response" ) #Not filling in on behalf of someone and not that person
        ],
        # fmt: on
        schema="id int, flushed string, survey_completed_dt string, first_name_confirmation string, first_name_on_behalf string, status string",
    )


def test_assign_survey_completed_status(spark_session, expected_df):
    expected_df = expected_df.withColumn("survey_completed_dt", F.to_timestamp(F.col("survey_completed_dt")))

    input_df = expected_df.drop(F.col("status"))

    actual_df = assign_survey_completed_status(
        input_df, "status", "survey_completed_dt", "flushed", ["first_name_confirmation", "first_name_on_behalf"]
    )

    assert_df_equality(actual_df, expected_df)
