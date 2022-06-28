from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_datetime_from_coalesced_columns_and_log_source


def test_assign_datetime_from_coalesced_columns_and_log_source(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (
                "2020-04-18 00:00:00",
                "2020-04-18 11:59:59",
                "2020-07-20 05:30:00",
                "2020-04-18 12:00:00",
                "2022-04-18 12:00:00",
                "date_1",
            ),  # no other times should be changed
            (
                None,
                "1999-04-18 11:59:59",
                "2020-07-20 15:59:59",
                "2020-07-20 15:59:59",
                "2022-04-18 12:00:00",
                "date_3",
            ),  # result should be priority 3 date as 1st is missing and 2nd is out of range
            (
                "2020-04-18 00:00:00",
                "2020-04-18 11:59:59",
                "2020-04-18 00:00:00",
                "2020-04-18 12:00:00",
                "2022-04-18 12:00:00",
                "date_1",
            ),  # chosen date has no time component so should be set to mid day
            (
                "2020-04-18 00:00:00",
                "2020-04-18 11:59:59",
                None,
                "2020-04-18 12:00:00",
                "2022-04-18 12:00:00",
                "date_1",
            ),  # null date in priority 3 should have no effect on result
            (
                None,  # null here ensures check for absence of timestamp ignores nulls
                None,
                "2020-04-18 00:00:00",
                "2020-04-18 00:00:00",
                "2022-04-18 12:00:00",
                "date_3",
            ),  # should pick priority 3 date when all others are null
            (None, None, None, None, "2022-04-18 12:00:00", None),  # no valid dates produces null from all null
            (
                "1999-04-18 00:00:00",
                "1999-04-18 00:00:00",
                "1999-04-18 00:00:00",
                None,
                "2022-04-18 12:00:00",
                None,
            ),  # should pick priority 3 date when all others are out of range
            (
                "2022-04-18 00:00:00",
                "2022-04-18 00:00:00",
                "2022-04-18 00:00:00",
                None,
                "2021-04-18 12:00:00",
                None,
            ),  # should be null as all dates are after file date
        ],
        schema="date_1 string, date_2 string, date_3 string, result string, file_date string, source string",
    )

    output_df = assign_datetime_from_coalesced_columns_and_log_source(
        expected_df.drop("result", "source"),
        column_name_to_assign="result",
        source_reference_column_name="source",
        ordered_columns=["date_1", "date_2", "date_3"],
        date_format="yyyy-MM-dd",
        time_format="HH:mm:ss",
        file_date_column="file_date",
        min_date="2000/05/01",
        default_timestamp="12:00:00",
    )
    for col in ["date_1", "date_2", "date_3", "result"]:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col, format="yyyy-MM-dd HH:mm:ss"))
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
