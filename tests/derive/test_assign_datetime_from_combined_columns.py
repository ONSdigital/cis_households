import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_datetime_from_combined_columns


def test_assign_datetime_from_combined_columns_no_date(spark_session):
    input_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            ("AM", "2020-01-01 00:00:00.000000", "1",   "15"),
            ("PM", "2020-01-01 00:00:00.000000", "1",   "15"),
            ("AM", "2020-01-01",                 "3",   "15"),
            (None, None,                         None,  None),
            (None, None,                         "1",   None),
            ("AM", None,                         "1",   None),
        ],
        # fmt: on
        schema="am_pm string, date string, hour string, minute string"
    )
    for col in ["date"]:
        input_df = input_df.withColumn(col, F.to_timestamp(col, format="yyyy-MM-dd"))
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            ("AM", "2020-01-01 00:00:00.000000",    "2020-01-01 01:15:00.0",    "1",    "15"),
            ("PM", "2020-01-01 00:00:00.000000",    "2020-01-01 13:15:00.0",    "1",    "15"),
            ("AM", "2020-01-01",                    "2020-01-01 03:15:00.0",    "3",    "15"),
            (None, None,                            None,                       None,   None),
            (None, None,                            None,                       "1",    None),
            ("AM", None,                            None,                       "1",    None),
        ],
        # fmt: on
        schema="am_pm string, date string, datetime string, hour string, minute string"
    )
    for col in ["date"]:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col, format="yyyy-MM-dd"))
    for col in ["datetime"]:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col, format="yyyy-MM-dd HH:mm:ss.S"))

    output_df = assign_datetime_from_combined_columns(
        df=input_df,
        column_name_to_assign="datetime",
        date_column="date",
        hour_column="hour",
        minute_column="minute",
        am_pm_column="am_pm",
    )

    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=False)
