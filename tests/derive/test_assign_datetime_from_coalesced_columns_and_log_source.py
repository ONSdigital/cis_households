from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_datetime_from_coalesced_columns_and_log_source


def test_assign_datetime_from_coalesced_columns_and_log_source(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (
                "2020-04-18 00:00:00.0",
                "2010-04-18 11:59:59.0",
                "2010-07-20 05:30:00.0",
                "2020-04-20 00:00:00.0",
                "2020-04-17 23:59:59.0",
                "2020-04-19 00:00:00.0",
                "2020-04-18 00:00:00.0",
                "2020-04-18 00:00:00.0",
                "date_1",
            ),  # no other times should be changed
            (
                None,
                "1999-04-18 11:59:59.0",
                "2020-07-20 15:59:59.0",
                None,
                "2020-07-20 15:59:59.0",
                "2020-07-20 15:59:59.0",
                "2020-07-20 15:59:59.0",
                "2020-07-10 00:00:00.0",
                "date_3",
            ),  # result should be priority 3 date as 1st is missing and 2nd is out of range
            (
                "2020-04-18 00:00:00.0",
                "2020-04-18 00:00:00.0",
                None,
                "2020-04-17 23:59:59.0",
                "2020-04-16 11:59:59.0",
                "2020-04-18 12:00:00.0",
                "2020-04-15 23:59:59.0",
                "2020-04-14 00:00:00.0",
                "ref_date",
            ),  # result should be ref_date - default_offset as primary dates out of range of reference date
            (
                "2020-04-18 00:00:00.0",
                "2020-04-18 00:00:00.0",
                "2020-04-15 00:00:00.0",
                "2020-04-16 23:59:59.0",
                "2020-04-17 11:59:59.0",
                "2020-04-18 12:00:00.0",
                "2020-04-14 23:59:59.0",
                "2020-04-10 00:00:00.0",
                "ref_date",
            ),  # primary dates out of range, ref_date minus offset also out of range but no validation on this
            (
                None,
                None,
                "2020-04-15 00:00:00.0",
                None,
                "2020-04-15 00:00:00.0",
                "2020-04-15 00:00:00.0",
                "2020-04-15 00:00:00.0",
                "2020-04-15 00:00:00.0",
                "date_3",
            ),  # should pick priority 3 date when all others are null
            (
                None,
                None,
                "2020-04-12 23:59:59.0",
                None,
                "2020-04-15 00:00:00.0",
                "2020-04-16 00:00:00.0",
                "2020-04-10 12:00:00.0",
                "2020-04-10 12:00:00.0",
                "fall_back_date",
            ),  # no valid dates so uses final fallback date
            (
                "2020-04-16 11:59:59.0",
                "2020-04-13 00:00:00.0",
                "2020-04-12 11:59:59.0",
                "2020-04-12 23:59:59.0",
                "2020-04-12 00:00:00.0",
                "2020-04-16 00:00:00.0",
                "2020-04-12 11:59:59.0",
                "2020-04-11 12:00:00.0",
                "date_3",
            ),  # should pick priority 3 date as date_1 above max and date_2 greater than ref_date
            (
                "2020-04-16 11:59:59.0",
                "2020-04-12 23:59:59.0",
                "2020-04-12 11:59:59.0",
                "2020-04-12 23:59:59.0",
                "2020-04-12 00:00:00.0",
                "2020-04-16 00:00:00.0",
                "2020-04-12 23:59:59.0",
                "2020-04-10 12:00:00.0",
                "date_2",
            ),  # as above but date_2 now within bounds so take as priority
        ],
        schema="date_1 string, date_2 string, date_3 string, ref_date string, min_date string, max_date string, result_date string, fall_back_date string, source string",
    )
    expected_df2 = spark_session.createDataFrame(
        data=[
            (
                "2020-04-22 00:00:00.0",
                None,
                "2020-04-18 00:00:00.0",
                "2020-04-20 00:00:00.0",
                "2020-04-17 23:59:59.0",
                "2020-04-19 00:00:00.0",
                "2020-04-18 12:00:00.0",
                "2020-04-16 00:00:00.0",
                "date_3",
            ),  # Pick date_3 and default to 12:00:00
            (
                None,
                None,
                "2020-07-20 00:00:00.0",
                "2020-07-20 11:59:59.0",
                "2020-07-18 00:00:00.0",
                "2020-07-22 00:00:00.0",
                "2020-07-20 12:00:00.0",
                "2020-07-19 12:00:00.0",
                "date_3",
            ),  # When defaulted to 12:00:00, date_3 no longer before ref_date but evaluated prior to setting default - THIS IS A FEATURE
            (
                "2020-07-12 12:00:00.0",
                "2020-07-30 12:00:00.0",
                "2020-07-18 00:00:00.0",
                "2020-07-20 12:00:01.0",
                "2020-07-22 12:59:59.0",
                "2020-07-23 00:00:00.0",
                "2020-07-19 12:00:01.0",
                "2020-07-12 12:00:00.0",
                "ref_date",
            ),  # No dates within bounds, choose ref_date - offset (even though also not satisfying min_date)
            (
                "2020-04-20 12:00:00.0",
                "2020-04-13 11:59:59.0",
                None,
                None,
                "2020-04-16 11:59:59.0",
                "2020-04-18 11:59:59.0",
                "2020-04-13 11:59:59.0",
                "2020-04-12 11:59:59.0",
                "date_2",
            ),  # Check new max_date and min_date with offsets have correct assignment
            (
                "2020-04-13 11:59:58.0",
                "2020-04-20 11:59:59.0",
                None,
                None,
                "2020-04-16 11:59:59.0",
                "2020-04-18 11:59:59.0",
                "2020-04-20 11:59:59.0",
                "2020-04-15 11:59:59.0",
                "date_2",
            ),  # Check new max_date and min_date with offsets have correct assignment
        ],
        schema="date_1 string, date_2 string, date_3 string, ref_date string, min_date string, max_date string, result_date string, fall_back_date string, source string",
    )
    for col in expected_df.columns:
        if "date" in col:
            expected_df = expected_df.withColumn(col, F.to_timestamp(F.col(col), format="yyyy-MM-dd HH:mm:ss.S"))
            expected_df2 = expected_df2.withColumn(col, F.to_timestamp(F.col(col), format="yyyy-MM-dd HH:mm:ss.S"))

    output_df = assign_datetime_from_coalesced_columns_and_log_source(
        expected_df.drop("result_date", "source"),
        column_name_to_assign="result_date",
        primary_datetime_columns=["date_1", "date_2", "date_3"],
        secondary_date_columns=[],
        min_datetime_column_name="min_date",
        max_datetime_column_name="max_date",
        reference_datetime_column_name="ref_date",
        min_datetime_offset_value=-2,
        max_datetime_offset_value=0,
        reference_datetime_days_offset_value=-2,
        source_reference_column_name="source",
        default_timestamp="12:00:00",
        final_fallback_column="fall_back_date",
    )
    output_df2 = assign_datetime_from_coalesced_columns_and_log_source(
        expected_df2.drop("result_date", "source"),
        column_name_to_assign="result_date",
        source_reference_column_name="source",
        primary_datetime_columns=["date_1", "date_2"],
        secondary_date_columns=["date_3"],
        min_datetime_column_name="min_date",
        max_datetime_column_name="max_date",
        reference_datetime_column_name="ref_date",
        min_datetime_offset_value=-3,
        max_datetime_offset_value=2,
        reference_datetime_days_offset_value=-1,
        default_timestamp="12:00:00",
        final_fallback_column="fall_back_date",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
    assert_df_equality(output_df2, expected_df2, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
