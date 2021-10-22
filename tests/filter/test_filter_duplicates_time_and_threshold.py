import datetime
import pyspark.sql.functions as F

import pytest
from chispa import assert_df_equality

from cishouseholds.filter import filter_duplicates_by_time_and_threshold


@pytest.fixture
def complete_df(spark_session):
    return spark_session.createDataFrame(
        data=[
            ("A", "B", datetime.datetime(2021, 8, 1, 10), 0.5, 1),  # original record
            ("A", "B", datetime.datetime(2021, 8, 1, 10), 0.5, 0),  # less than time thresh, less than float thresh
            # less than time thresh, less than float thresh
            ("A", "B", datetime.datetime(2021, 8, 1, 11), 0.500000000001, 0),
            ("A", "B", datetime.datetime(2021, 8, 1, 12), 0.5, 1),  # greater than time thresh, less than float thresh
            ("A", "B", datetime.datetime(2021, 8, 1, 10), 0.51, 1),  # less than time thresh, greater than float thresh
        ],
        schema=["v1", "v2", "v3", "v4", "kept"],
    )


def test_filter_duplicates_time_and_threshold(spark_session, complete_df):
    expected_df = complete_df.filter(F.col("kept") == 1).drop("kept")
    input_df = complete_df.drop("kept")

    actual_df = filter_duplicates_by_time_and_threshold(input_df, "v1", "v2", "v3", "v4", 1.5, 0.00001)
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)
