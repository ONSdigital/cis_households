from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_datetime_from_coalesced_columns_and_log_source


def test_assign_datetime_from_coalesced_columns_and_log_source(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (
                "2020-04-18 00:00:00.0",
                "2020-04-18 11:59:59.0",
                "2020-07-20 05:30:00.0",
                "2020-04-18 12:00:00.0",
                "2022-04-18 12:00:00.0",
                "date_1",
            ),  # no other times should be changed
            (
                None,
                "1999-04-18 11:59:59.0",
                "2020-07-20 15:59:59.0",
                "2020-07-20 15:59:59.0",
                "2022-04-18 12:00:00.0",
                "date_3",
            ),  # result should be priority 3 date as 1st is missing and 2nd is out of range
            (
                "2020-04-18 00:00:00.0",
                "2020-04-18 11:59:59.0",
                "2020-04-18 00:00:00.0",
                "2020-04-18 12:00:00.0",
                "2022-04-18 12:00:00.0",
                "date_1",
            ),  # chosen date has no time component so should be set to mid day
            (
                "2020-04-18 00:00:00.0",
                "2020-04-18 11:59:59.0",
                None,
                "2020-04-18 12:00:00.0",
                "2022-04-18 12:00:00.0",
                "date_1",
            ),  # null date in priority 3 should have no effect on result
            (
                None,  # null here ensures check for absence of timestamp ignores nulls
                None,
                "2020-04-18 00:00:00.0",
                "2020-04-18 00:00:00.0",
                "2022-04-18 12:00:00.0",
                "date_3",
            ),  # should pick priority 3 date when all others are null
            (None, None, None, None, "2022-04-18 12:00:00.0", None),  # no valid dates produces null from all null
            (
                "1999-04-18 00:00:00.0",
                "1999-04-18 00:00:00.0",
                "1999-04-18 00:00:00.0",
                None,
                "2022-04-18 12:00:00.0",
                None,
            ),  # should pick priority 3 date when all others are out of range
            (
                "2022-04-18 00:00:00.0",
                "2022-04-18 00:00:00.0",
                "2022-04-18 00:00:00.0",
                None,
                "2021-04-18 12:00:00.0",
                None,
            ),  # should be null as all dates are after file date
        ],
        schema="date_1 string, date_2 string, date_3 string, result_date string, file_date string, source string",
    )
    expected_df2 = spark_session.createDataFrame(
        data=[
            (
                "18-04-2020 00:00:00.0",
                "18-04-2020 11:59:59.0",
                "20-07-2020 05:30:00.0",
                "18-04-2020 12:00:00.0",
                "18-04-2020 12:00:00.0",
                "date_1",
            ),  # no other times should be changed
            (
                None,
                "18-04-1999 11:59:59.0",
                "20-07-2020 15:59:59.0",
                "20-07-2020 15:59:59.0",
                "18-04-2022 12:00:00.0",
                "date_3",
            ),  # result should be priority 3 date as 1st is missing and 2nd is out of range
            (
                "18-04-2020 00:00:00.0",
                "18-04-2020 11:59:59.0",
                "18-04-2020 00:00:00.0",
                "18-04-2020 12:00:00.0",
                "18-04-2020 12:00:00.0",
                "date_1",
            ),  # chosen date has no time component so should be set to mid day
            (
                "18-04-2020 00:00:00.0",
                "18-04-2020 11:59:59.0",
                None,
                "18-04-2020 12:00:00.0",
                "18-04-2020 12:00:00.0",
                "date_1",
            ),  # null date in priority 3 should have no effect on result
            (
                None,  # null here ensures check for absence of timestamp ignores nulls
                None,
                "18-04-2020 00:00:00.0",
                "18-04-2020 00:00:00.0",
                "18-04-2020 12:00:00.0",
                "date_3",
            ),  # should pick priority 3 date when all others are null
            (None, None, None, None, "18-04-2022 12:00:00.0", None),  # no valid dates produces null from all null
            (
                "18-04-1999 00:00:00.0",
                "18-04-1999 00:00:00.0",
                "18-04-1999 00:00:00.0",
                None,
                "18-04-2022 12:00:00.0",
                None,
            ),  # should pick priority 3 date when all others are out of range
            (
                "18-04-2022 00:00:00.0",
                "18-04-2022 00:00:00.0",
                "18-04-2022 00:00:00.0",
                None,
                "18-04-2021 12:00:00.0",
                None,
            ),  # should be null as all dates are after file date
        ],
        schema="date_1 string, date_2 string, date_3 string, result_date string, file_date string, source string",
    )
    for col in expected_df.columns:
        if "date" in col:
            expected_df = expected_df.withColumn(col, F.to_timestamp(F.col(col), format="yyyy-MM-dd HH:mm:ss.S"))
            expected_df2 = expected_df2.withColumn(col, F.to_timestamp(F.col(col), format="dd-MM-yyyy HH:mm:ss.S"))

    output_df = assign_datetime_from_coalesced_columns_and_log_source(
        expected_df.drop("result", "source"),
        column_name_to_assign="result_date",
        source_reference_column_name="source",
        ordered_columns=["date_1", "date_2", "date_3"],
        file_date_column="file_date",
        min_date="2000/05/01",
        default_timestamp="12:00:00",
    )
    output_df2 = assign_datetime_from_coalesced_columns_and_log_source(
        expected_df2.drop("result", "source"),
        column_name_to_assign="result_date",
        source_reference_column_name="source",
        ordered_columns=["date_1", "date_2", "date_3"],
        file_date_column="file_date",
        min_date="2000/05/01",
        default_timestamp="12:00:00",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
    assert_df_equality(output_df2, expected_df2, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
