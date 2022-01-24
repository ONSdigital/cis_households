import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.merge_process import execute_merge_specific_antibody

# from pyspark.sql import functions as F
# from pyspark.sql import Window


def test_merge_process_antibody(spark_session):
    schema = "barcode string, comments_surv string"
    data = [
        ("A", "1to1"),
        ("B", "mto1"),
        ("C", "1tom"),
        ("C", "1tom"),
        ("C", "1tom"),
        ("D", "mtom"),
        ("D", "mtom"),
        ("D", "mtom"),
        ("E", "1to1"),
        ("F", "mto1"),
        ("G", "1tom"),
        ("G", "1tom"),
        ("H", "mtom"),
        ("H", "mtom"),
        ("J", "1to1"),
        ("K", "mto1"),
        ("L", "1tom"),
        ("L", "1tom"),
        ("M", "mtom"),
        ("M", "mtom"),
        ("N", "mto1"),
        ("P", "1tom"),
        ("P", "1tom"),
        ("Q", "mtom"),
        ("Q", "mtom"),
        ("R", "1to1"),
        ("S", "1to1"),
        ("T", "1to1"),
        ("U", "1to1"),
        ("V", "1to1"),
        ("W", "1to1"),
        ("X", "1to1"),
    ]
    df_input_survey = spark_session.createDataFrame(data, schema=schema)

    schema = """
                barcode string,
                date_visit string,
                date_received string,
                antibody_result_recorded_datetime string,
                antibody_result_classification string,
                tdi string,
                siemens string,
                testing string
            """
    data = [
        ("A", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "merge_type"),
        ("B", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "merge_type"),
        ("B", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "merge_type"),
        ("B", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "merge_type"),
        ("C", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "merge_type"),
        ("D", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "merge_type"),
        ("D", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "merge_type"),
        ("D", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "merge_type"),
        ("E", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "out of range"),
        ("F", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "out of range"),
        ("F", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "out of range"),
        ("G", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "out of range"),
        ("H", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "out of range"),
        ("H", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "out of range"),
        (
            "J",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "NOT out of range",
        ),
        (
            "K",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "NOT out of range",
        ),
        (
            "K",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "NOT out of range",
        ),
        (
            "L",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "NOT out of range",
        ),
        (
            "M",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "NOT out of range",
        ),
        (
            "M",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "NOT out of range",
        ),
        (
            "N",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "time_order_flag - closest to visit",
        ),
        (
            "N",
            "2020-01-01",
            "2020-01-03",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "time_order_flag - NOT closest to visit",
        ),
        (
            "P",
            "2020-01-01",
            "2020-01-04",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "time_order_flag - NOT closest to visit",
        ),
        (
            "Q",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "time_order_flag - closest to visit",
        ),
        (
            "Q",
            "2020-01-01",
            "2020-01-03",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "time_order_flag - NOT closest to visit",
        ),
        ("R", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "antibody_flag"),
        ("S", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "negative", "positive", "positive", "antibody_flag"),
        ("T", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "void", "positive", "positive", "antibody_flag"),
        ("U", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "tdi"),
        ("V", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "negative", "positive", "tdi"),
        ("W", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "siemens"),
        ("X", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "negative", "siemens"),
    ]
    df_input_labs = spark_session.createDataFrame(data, schema=schema)

    schema = """

            """

    data = []
    expected_df = spark_session.createDataFrame(data, schema=schema)

    df = execute_merge_specific_antibody(
        survey_df=df_input_survey,
        labs_df=df_input_labs,
        barcode_column_name="barcode",
        visit_date_column_name="date_visit",
        received_date_column_name="date_received",
    )

    assert_df_equality(
        df,
        expected_df,
        ignore_column_order=True,
        ignore_row_order=True,
    )
