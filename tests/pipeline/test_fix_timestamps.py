import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import fix_timestamps


def test_fix_timestamps(spark_session):
    schema = "a_datetime string, b_date string"
    input_df = spark_session.createDataFrame(
        data=[
            ("2022-05-17 12:00:00", "2022-08-04 12:00:00"),
            ("2022-08-01 12:00:00", "2022-08-04 12:00:00"),
            ("2022-08-11 12:00:00", "2022-08-04 12:00:00"),  # only 1 datetime present
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("2022-05-17 12:00:00", "2022-08-04"),
            ("2022-08-01 12:00:00", "2022-08-04"),
            ("2022-08-11 12:00:00", "2022-08-04"),  # only 1 datetime present
        ],
        schema=schema,
    )

    for col in input_df.columns:
        input_df = input_df.withColumn(col, F.to_timestamp(col))

    expected_df = expected_df.withColumn("a_datetime", F.to_timestamp("a_datetime"))

    output_df = fix_timestamps(input_df)
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
