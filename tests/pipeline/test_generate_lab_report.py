import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import generate_lab_report


def test_generate_lab_report(spark_session):
    input_df = spark_session.createDataFrame(
        data=[("2022-05-17", "1", "1", "1"), ("2022-08-01", "2", "2", "2")],
        schema="file_date string, swab_sample_barcode string, swab_taken_datetime string, survey_completed_datetime string",
    )
    expected_df = spark_session.createDataFrame(
        data=[("2", "2", "2")],
        schema="swab_sample_barcode string, swab_taken_datetime string, survey_completed_datetime string",
    )
    input_df = input_df.withColumn("file_date", F.to_timestamp(F.col("file_date"), format="yyyy-MM-dd"))
    output_df = generate_lab_report(input_df)

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
