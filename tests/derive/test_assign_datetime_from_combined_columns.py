import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_datetime_from_combined_columns


@pytest.mark.parametrize(
    "expected_data",
    [
        ("2020-01-01", 12, 30, 16, "pm", "2020-01-01 12:30:16"),
        ("2020-01-01", 0, 30, 16, "am", "2020-01-01 00:30:16"),
        ("2020-01-01", 3, 30, 16, "pm", "2020-01-01 15:30:16"),
        ("2020-01-01", 11, 0, 0, "am", "2020-01-01 11:00:00"),
    ],
)
def test_assign_datetime_from_combined_columns(spark_session, expected_data):
    schema = "date string, hour integer, minute integer, second integer, am_pm string, datetime string"
    expected_df = spark_session.createDataFrame(data=[expected_data], schema=schema)

    input_df = expected_df.drop("datetime")

    expected_df = expected_df.withColumn("datetime", F.to_timestamp("datetime")).drop("hour", "minute", "second")
    output_df = assign_datetime_from_combined_columns(
        df=input_df,
        column_name_to_assign="datetime",
        date_column="date",
        hour_column="hour",
        minute_column="minute",
        second_column="second",
        am_pm_column="am_pm",
    )
    assert_df_equality(output_df, expected_df)
