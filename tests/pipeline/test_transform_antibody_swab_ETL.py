import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.merge_antibody_swab_ETL import transform_antibody_swab_ETL

# from pyspark.sql import functions as F


@pytest.mark.xfail(reason="Expected DF need creating")
def test_transform_antibody_swab_ETL(spark_session):
    # df survey data
    schema = "blood_barcode string, blood_barcode string, visit_date_string string"
    data = [
        ("A", "A", "2020-01-01"),
        ("A", "A", "2020-01-01"),
        ("B", "B", "2020-01-01"),
        ("B", "B", "2020-01-01"),
        ("C", "C", "2020-01-01"),
        ("C", "C", "2020-01-01"),
        ("D", "D", "2020-01-01"),
        ("D", "D", "2020-01-01"),
    ]
    df_input_survey = spark_session.createDataFrame(data, schema=schema)

    # df antibody
    schema = """
                barcode string,
                date_received string,
                antibody_result_recorded_datetime string,
                antibody_result_classification string,
                tdi string,
                siemens string,
            """
    data = [
        ("A", "2020-01-01", "2020-01-04 12:00:00", "positive", "positive", "positive"),
        ("C", "2020-01-01", "2020-01-04 12:00:00", "positive", "positive", "positive"),
        ("E", "2020-01-01", "2020-01-04 12:00:00", "positive", "positive", "positive"),
    ]
    df_input_antibody = spark_session.createDataFrame(data, schema=schema)

    # df swab
    schema = """
                barcode string,
                date_visit string,
                date_received string,
                pcr_result_recorded_datetime string,
                pcr_result_classification string,
                comments_lab string
            """

    data = [
        ("A", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive"),
        ("B", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive"),
        ("F", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive"),
    ]
    df_input_swabs = spark_session.createDataFrame(data, schema=schema)

    # df expected antibody_swab merged data
    schema = """"""

    data = []
    expected_antibody_swab_merged_df = spark_session.createDataFrame(data, schema=schema)

    # df expected antibody residuals
    schema = """"""

    data = []
    expected_antibody_residuals_df = spark_session.createDataFrame(data, schema=schema)

    # df expected antibody_swab residuals
    schema = """"""

    data = []
    expected_antibody_swab_residuals_df = spark_session.createDataFrame(data, schema=schema)

    (
        _,
        survey_antibody_residuals,
        _,
        survey_antibody_swab_df,
        survey_antibody_swab_residuals,
        _,
    ) = transform_antibody_swab_ETL(df_input_survey, df_input_antibody, df_input_swabs)

    assert_df_equality(
        survey_antibody_swab_df, expected_antibody_swab_merged_df, ignore_row_order=True, ignore_column_order=True
    )
    assert_df_equality(
        survey_antibody_residuals, expected_antibody_residuals_df, ignore_row_order=True, ignore_column_order=True
    )
    assert_df_equality(
        survey_antibody_swab_residuals,
        expected_antibody_swab_residuals_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
