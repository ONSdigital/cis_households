from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_datetime_from_group


def test_assign_datetime_from_group(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (
                "2020-04-18 00:00:00",
                "2020-04-18 11:59:59",
                "2020-07-20 05:30:00",
                "2020-04-18 12:00:00",
                "2022-04-18 12:00:00",
            ),
            (
                "2020-04-18 00:00:00",
                "2020-04-18 11:59:59",
                "2020-07-20 15:59:59",
                "2020-04-18 12:00:00",
                "2022-04-18 12:00:00",
            ),
            (
                "2020-04-18 00:00:00",
                "2020-04-18 11:59:59",
                "2020-04-18 00:00:00",
                "2020-04-18 12:00:00",
                "2022-04-18 12:00:00",
            ),
            (None, None, "2020-04-18 00:00:00", "2020-04-18 00:00:00", "2022-04-18 12:00:00"),
        ],
        schema="date_1 string, date_2 string, date_3 string, result string, file_date string",
    )
    expected_df = expected_df.withColumn("result", F.to_timestamp("result", format="yyyy-MM-dd HH:mm:ss"))
    output_df_1 = assign_datetime_from_group(
        expected_df.drop("result"),
        column_name_to_assign="result",
        ordered_columns=["date_1", "date_2", "date_3"],
        date_format="yyyy-MM-dd",
        file_date_column="file_date",
        time_format="HH:mm:ss",
        default_timestamp="12:00:00",
    )
    assert_df_equality(output_df_1, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)

    for col in expected_df.columns:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col, format="yyyy-MM-dd HH:mm:ss"))
    output_df_2 = assign_datetime_from_group(
        expected_df.drop("result"),
        column_name_to_assign="result",
        ordered_columns=["date_1", "date_2", "date_3"],
        date_format="yyyy-MM-dd",
        file_date_column="file_date",
        time_format="HH:mm:ss",
        default_timestamp="12:00:00",
    )
    assert_df_equality(output_df_2, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
