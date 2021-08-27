import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.filter import flag_out_of_date_range


def test_flag_out_of_date_range(spark_session):
    expected_schema = "date_1 string, date_2 string, outside_interval_flag integer"
    expected_data_h = [
        # difference to be out of range - up
        ("2020-01-01 12:00:00", "2020-01-04 12:00:00", 1),
        # difference to be out of range - down
        ("2020-01-01 12:00:00", "2019-12-20 12:00:00", 1),
        # difference within range - down
        ("2020-01-01 12:00:00", "2020-01-01 11:00:00", None),
        # difference within range - up
        ("2020-01-01 12:00:00", "2020-01-01 13:00:00", None),
        # missing date 2 - nullable
        ("2020-01-01 12:00:00", None, None),
    ]
    expected_df = (
        spark_session.createDataFrame(expected_data_h, schema=expected_schema)
        .withColumn("date_1", F.to_timestamp(F.col("date_1")))
        .withColumn("date_2", F.to_timestamp(F.col("date_2")))
    )

    input_df = expected_df.drop("outside_interval_flag")

    # GIVEN UPPER/LOWER INTERVALS IN HOURS (STANDARD)
    actual_df = flag_out_of_date_range(input_df, "outside_interval_flag", "date_1", "date_2", -12, 48)
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)

    # GIVEN UPPER/LOWER INTERVALS IN DAYS
    actual_df = flag_out_of_date_range(input_df, "outside_interval_flag", "date_1", "date_2", -0.5, 2, "days")
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
