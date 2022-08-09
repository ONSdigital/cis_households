import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import generate_lab_report


def test_generate_lab_report(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("2022-05-17", "1", "1", "1", "1"),
            ("2022-08-01", "2", "2", "2", "2"),
            ("2022-08-11", "3", None, "3", "3"),  # only 1 datetime present
            ("2022-08-09", None, "4", "4", None),  # blood only records
            (None, "5", None, None, None),  # no datetime present
        ],
        schema="survey_completed_datetime string, swab_sample_barcode string, blood_taken_datetime string, blood_sample_barcode string, swab_taken_datetime string, survey_completed_datetime string",
    )
    expected_swab_df = spark_session.createDataFrame(
        data=[("2", "2", "2"), ("3", "3", "3")],
        schema="swab_sample_barcode string, swab_taken_datetime string, survey_completed_datetime string",
    )
    expected_blood_df = spark_session.createDataFrame(
        data=[("2", "2", "2"), ("3", None, "3"), ("4", "4", "4")],
        schema="blood_sample_barcode string, blood_taken_datetime string, survey_completed_datetime string",
    )
    input_df = input_df.withColumn(
        "survey_completed_datetime", F.to_timestamp(F.col("survey_completed_datetime"), format="yyyy-MM-dd")
    )
    output_swab_df, output_blood_df = generate_lab_report(
        input_df, F.to_timestamp(F.lit("2022-08-04"), format="yyyy-MM-dd")
    )

    assert_df_equality(output_swab_df, expected_swab_df, ignore_nullable=True, ignore_row_order=True)
    assert_df_equality(output_blood_df, expected_blood_df, ignore_nullable=True, ignore_row_order=True)
