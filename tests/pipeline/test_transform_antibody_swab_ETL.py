import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_blood
from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_swab


@pytest.mark.xfail(reason="Test runs, but expected data need defining")
def test_transform_antibody_swab_ETL(spark_session):
    # df survey data
    survey_schema = """
        swab_sample_barcode string,
        blood_sample_barcode string,
        visit_date_string string,
        visit_datetime string
    """
    survey_data = [
        ("A", "A", "2020-01-01", "2020-01-01"),
        ("A", "A", "2020-01-01", "2020-01-01"),
        ("B", "B", "2020-01-01", "2020-01-01"),
        ("B", "B", "2020-01-01", "2020-01-01"),
        ("C", "C", "2020-01-01", "2020-01-01"),
        ("C", "C", "2020-01-01", "2020-01-01"),
    ]
    df_input_survey = spark_session.createDataFrame(survey_data, schema=survey_schema)

    swab_schema = """
                swab_sample_barcode string,
                pcr_result_recorded_datetime string,
                pcr_result_classification string
                """
    swab_data = [
        ("A", "2020-01-04 12:00:00", "positive"),
        ("C", "2020-01-04 12:00:00", "positive"),
        ("E", "2020-01-04 12:00:00", "positive"),
    ]
    df_input_swab = spark_session.createDataFrame(swab_data, schema=swab_schema)

    blood_schema = """
                antibody_test_well_id string,
                antibody_test_plate_id string,
                blood_sample_barcode string,
                blood_sample_received_date string,
                blood_sample_arrayed_date string,
                antibody_test_result_recorded_date string,
                antibody_test_result_classification string,
                assay_category string
            """
    blood_data = [
        # fmt: off
        ("w1","p1","A","2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",  1.1,1.1,None,"positive","positive","V", None,None,None,None,None),
        ("w1","p2","B","2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",  1.2,1.2,None,"positive","positive","C", None,None,None,None,None),
        ("w1","p3","F","2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",  1.3,1.3,None,"positive","positive","V", None,None,None,None,None),
        # fmt: on
    ]
    df_input_antibody = spark_session.createDataFrame(blood_data, schema=blood_schema)

    expected_schema = """
                visit_datetime string,
                blood_sample_barcode string,
                blood_sample_received_date string,
                blood_sample_arrayed_date string,
                antibody_result_classification string,
                assay_tdi string,
                assay_siemens string,
                date_visit string,
                date_received string,
                pcr_result_recorded_datetime string,
                pcr_result_classification string,
                blood_sample_type string,
                swab_sample_barcode string,
                blood_test_source_file string
                """

    expected_data = [
        # fmt: off
        ("A","2020-01-01","2020-01-02","2020-01-04 12:00:00","positive","positive","positive",None,None,None,None,None,None,None),
        ("B","2020-01-01","2020-01-02","2020-01-04 12:00:00","positive","positive","positive",None,None,None,None,None,None,None),
        # fmt: on
    ]
    expected_antibody_swab_merged_df = spark_session.createDataFrame(expected_data, schema=expected_schema)

    # # df expected antibody residuals
    # schema = """"""

    # data = []
    # expected_antibody_residuals_df = spark_session.createDataFrame(data, schema=schema)

    # # df expected antibody_swab residuals
    # schema = """"""

    # data = []
    # expected_antibody_swab_residuals_df = spark_session.createDataFrame(data, schema=schema)

    import pdb

    pdb.set_trace()
    responses_with_blood_df, blood_residuals, _ = merge_blood(df_input_survey, df_input_antibody)
    responses_with_blood_and_swab_df, swab_residuals, _ = merge_swab(responses_with_blood_df, df_input_swab)

    assert_df_equality(
        responses_with_blood_and_swab_df,
        expected_antibody_swab_merged_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
    # assert_df_equality(
    #     blood_residuals,
    #     expected_antibody_residuals_df,
    #     ignore_row_order=True,
    #     ignore_column_order=True
    #     )
    # assert_df_equality(
    #     swab_residuals,
    #     expected_antibody_swab_residuals_df,
    #     ignore_row_order=True,
    #     ignore_column_order=True,
    # )
