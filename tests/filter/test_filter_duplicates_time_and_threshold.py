import datetime

import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.filter import filter_duplicates_by_time_and_threshold


@pytest.fixture
def complete_df(spark_session):
    return spark_session.createDataFrame(
        data=[
            ("A", "B", datetime.datetime(2021, 8, 1, 10), 0.5, 1),
            ("A", "B", datetime.datetime(2021, 8, 1, 10), 0.5, 0),
            ("A", "B", datetime.datetime(2021, 8, 1, 11), 0.5, 0),  # time thresh less than
            ("A", "B", datetime.datetime(2021, 8, 1, 12), 0.5, 1),  # time thresh greater than
        ],
        schema=["v1", "v2", "v3", "v4", "kept"],
    )


def test_filter_duplicates_time_and_threshold(spark_session, complete_df):
    expected_df = complete_df.filter(F.col("kept") == 1).drop("kept")
    input_df = complete_df.drop("kept")

    actual_df = filter_duplicates_by_time_and_threshold(input_df, "v1", "v2", "v3", "v4", 1.5)
    assert_df_equality(actual_df, expected_df)
