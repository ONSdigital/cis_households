import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.merge_process import execute_merge_specific_antibody

# from pyspark.sql import functions as F
# from pyspark.sql import Window


@pytest.mark.xfail(reason="units do not function correctly")
def test_merge_process_antibody(spark_session):
    schema = "barcode string, comments_surv string, unique_participant_response_id string"
    data = [
        ("A", "1to1", "1S"),
        ("B", "mto1", "2S"),
        ("C", "1tom", "3S"),
        ("C", "1tom", "4S"),
        ("C", "1tom", "5S"),
        ("D", "mtom", "6S"),
        ("D", "mtom", "7S"),
        ("D", "mtom", "8S"),
        ("E", "1to1", "9S"),
        ("F", "mto1", "10S"),
        ("G", "1tom", "11S"),
        ("G", "1tom", "12S"),
        ("H", "mtom", "13S"),
        ("H", "mtom", "14S"),
        ("J", "1to1", "15S"),
        ("K", "mto1", "16S"),
        ("L", "1tom", "17S"),
        ("L", "1tom", "18S"),
        ("M", "mtom", "19S"),
        ("M", "mtom", "20S"),
        ("N", "mto1", "21S"),
        ("P", "1tom", "22S"),
        ("P", "1tom", "23S"),
        ("Q", "mtom", "24S"),
        ("Q", "mtom", "25S"),
        ("R", "1to1", "26S"),
        ("S", "1to1", "27S"),
        ("T", "1to1", "28S"),
        ("U", "1to1", "29S"),
        ("V", "1to1", "30S"),
        ("W", "1to1", "31S"),
        ("X", "1to1", "32S"),
    ]
    df_input_survey = spark_session.createDataFrame(data, schema=schema)

    schema = """
                barcode string,
                unique_antibody_test_id string,
                date_visit string,
                date_received string,
                antibody_result_recorded_datetime string,
                antibody_test_result_classification string,
                tdi string,
                assay_siemens string,
                testing string
            """
    data = [
        (
            "A",
            "1L",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "merge_type",
        ),
        (
            "B",
            "2L",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "merge_type",
        ),
        (
            "B",
            "3L",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "merge_type",
        ),
        (
            "B",
            "4L",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "merge_type",
        ),
        (
            "C",
            "5L",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "merge_type",
        ),
        (
            "D",
            "6L",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "merge_type",
        ),
        (
            "D",
            "7L",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "merge_type",
        ),
        (
            "D",
            "8L",
            "2020-01-01",
            "2020-01-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "merge_type",
        ),
        (
            "E",
            "9L",
            "2020-01-01",
            "2020-06-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "out of range",
        ),
        (
            "F",
            "10L",
            "2020-01-01",
            "2020-06-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "out of range",
        ),
        (
            "F",
            "11L",
            "2020-01-01",
            "2020-06-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "out of range",
        ),
        (
            "G",
            "12L",
            "2020-01-01",
            "2020-06-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "out of range",
        ),
        (
            "H",
            "13L",
            "2020-01-01",
            "2020-06-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "out of range",
        ),
        (
            "H",
            "14L",
            "2020-01-01",
            "2020-06-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "out of range",
        ),
        (
            "J",
            "15L",
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
            "16L",
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
            "17L",
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
            "18L",
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
            "19L",
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
            "20L",
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
            "21L",
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
            "22L",
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
            "23L",
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
            "24L",
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
            "25L",
            "2020-01-01",
            "2020-01-03",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "time_order_flag - NOT closest to visit",
        ),
        (
            "R",
            "26L",
            "2020-01-01",
            "2020-06-02",
            "2020-01-04 12:00:00",
            "positive",
            "positive",
            "positive",
            "antibody_flag",
        ),
        (
            "S",
            "27L",
            "2020-01-01",
            "2020-06-02",
            "2020-01-04 12:00:00",
            "negative",
            "positive",
            "positive",
            "antibody_flag",
        ),
        (
            "T",
            "28L",
            "2020-01-01",
            "2020-06-02",
            "2020-01-04 12:00:00",
            "void",
            "positive",
            "positive",
            "antibody_flag",
        ),
        ("U", "29L", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "tdi"),
        ("V", "30L", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "negative", "positive", "tdi"),
        ("W", "31L", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "positive", "siemens"),
        ("X", "32L", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "positive", "negative", "siemens"),
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
