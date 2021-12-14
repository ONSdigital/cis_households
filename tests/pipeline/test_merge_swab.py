from chispa import assert_df_equality

from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_swab


def test_merge_swab(spark_session):
    voyager_schema = """
    unique_participant_response_id string,
    swab_sample_barcode string,
    visit_datetime string,
    iqvia_column string
    """
    voyager_data = [
        # fmt: off
        ("IQVIA-1","ONS001","2020-08-01 01:00:00","i_data"),
        ("IQVIA-2","ONS002","2020-08-01 01:00:00","i_data"),
        ("IQVIA-3","ONS003","2020-08-01 01:00:00","i_data"),
        ("IQVIA-4","ONS004","2020-08-01 01:00:00","i_data"),
        ("IQVIA-5","ONS005","2020-08-01 01:00:00","i_data"),
        ("IQVIA-6","ONS005","2020-08-04 01:00:00","i_data"),
        ("IQVIA-7","ONS006","2020-08-01 01:00:00","i_data"),
        ("IQVIA-8","ONS006","2020-08-01 02:00:00","i_data"),
        ("IQVIA-9","ONS007","2020-08-01 01:00:00","i_data"),
        ("IQVIA-10","ONS007","2020-08-02 01:00:00","i_data"),
        ("IQVIA-11","ONS008","2020-08-01 01:00:00","i_data"),
        ("IQVIA-12","ONS008","2020-08-07 01:00:00","i_data"),
        ("IQVIA-13","ONS009","2020-08-01 01:00:00","i_data"),
        ("IQVIA-14","ONS009","2020-08-02 01:00:00","i_data"),
        ("IQVIA-15","ONS010","2020-08-01 01:00:00","i_data"),
        # fmt: on
    ]
    voyager_df = spark_session.createDataFrame(voyager_data, schema=voyager_schema)

    swab_schema = """
        unique_pcr_test_id string,
        swab_sample_barcode string,
        pcr_result_recorded_datetime string,
        pcr_result_classification string,
        merge_type_info string
    """
    swab_data = [
        # fmt: off
        ("SWABS-1","ONS001","2020-08-02 00:00:00","Positive", "1:1 success"),
        ("SWABS-2","ONS002","2020-08-22 00:00:00","Negative", "1:1 out-of-range"),
        ("SWABS-3","ONS003","2020-08-03 00:00:00","Positive", "1:m success"),
        ("SWABS-4","ONS003","2020-08-04 00:00:00","Positive", "1:m dropped"),
        ("SWABS-5","ONS004","2020-08-11 00:00:00","Negative", "1:m success"),
        ("SWABS-6","ONS004","2020-08-09 00:00:00","Void", "1:m dropped"),
        ("SWABS-7","ONS005","2020-08-02 00:00:00","Positive", "m:1 success"),
        ("SWABS-8","ONS006","2020-08-03 00:00:00","Positive", "m:1 dropped"),
        ("SWABS-9","ONS007","2020-08-06 00:00:00","Negative", "m:m success"),
        ("SWABS-10","ONS007","2020-08-08 00:00:00","Negative", "m:m success"),
        ("SWABS-11","ONS008","2020-08-03 00:00:00","Positive", "m:m success"),
        ("SWABS-12","ONS008","2020-08-04 00:00:00","Positive", "m:m dropped"),
        ("SWABS-13","ONS009","2020-08-02 00:00:00","Positive", "m:m failed"),
        ("SWABS-14","ONS009","2020-08-03 00:00:00","Negative", "m:m failed"),
        ("SWABS-15","ONS011","2020-08-04 00:00:00","Positive", "none:1"),
        # fmt: on
    ]
    swab_df = spark_session.createDataFrame(swab_data, schema=swab_schema)

    expected_voyager_swab_schema = """
        unique_participant_response_id string,
        swab_sample_barcode string,
        visit_datetime string,
        iqvia_column string,
        unique_pcr_test_id string,
        pcr_result_recorded_datetime string,
        pcr_result_classification string,
        merge_type_info string
    """
    expected_voyager_swab_data = [
        # fmt: off
        ("IQVIA-1","ONS001","2020-08-01 01:00:00","i_data",      "SWABS-1","2020-08-02 00:00:00","Positive", "1:1 success"),
        ("IQVIA-2","ONS002","2020-08-01 01:00:00","i_data",      None, None, None, None),
        ("IQVIA-3","ONS003","2020-08-01 01:00:00","i_data",      "SWABS-3","2020-08-03 00:00:00","Positive", "1:m success"),
        ("IQVIA-4","ONS004","2020-08-01 01:00:00","i_data",      "SWABS-5","2020-08-11 00:00:00","Negative", "1:m success"),
        ("IQVIA-5","ONS005","2020-08-01 01:00:00","i_data",      "SWABS-7","2020-08-02 00:00:00","Positive", "m:1 success"),
        ("IQVIA-6","ONS005","2020-08-04 01:00:00","i_data",      None, None, None, None),
        ("IQVIA-7","ONS006","2020-08-01 01:00:00","i_data",      None, None, None, None),
        ("IQVIA-8","ONS006","2020-08-01 02:00:00","i_data",      None, None, None, None),
        ("IQVIA-9","ONS007","2020-08-01 01:00:00","i_data",      "SWABS-10","2020-08-08 00:00:00","Negative", "m:m success"),
        ("IQVIA-10","ONS007","2020-08-02 01:00:00","i_data",     "SWABS-9","2020-08-06 00:00:00","Negative", "m:m success"),
        ("IQVIA-11","ONS008","2020-08-01 01:00:00","i_data",     "SWABS-11","2020-08-03 00:00:00","Positive", "m:m success"),
        ("IQVIA-12","ONS008","2020-08-07 01:00:00","i_data",     None, None, None, None),
        ("IQVIA-13","ONS009","2020-08-01 01:00:00","i_data",     None, None, None, None),
        ("IQVIA-14","ONS009","2020-08-02 01:00:00","i_data",     None, None, None, None),
        ("IQVIA-15","ONS010","2020-08-01 01:00:00","i_data",     None, None, None, None),
        # fmt: on
    ]
    expected_df_voyager_swab = spark_session.createDataFrame(
        expected_voyager_swab_data, schema=expected_voyager_swab_schema
    )

    expected_df_swab_residuals_schema = """
        unique_pcr_test_id string,
        swab_sample_barcode string,
        pcr_result_recorded_datetime string,
        pcr_result_classification string,
        merge_type_info string
    """
    expected_df_swab_residuals_data = [
        # fmt: off
        ("SWABS-2","ONS002","2020-08-22 00:00:00","Negative", "1:1 out-of-range"),
        ("SWABS-4","ONS003","2020-08-04 00:00:00","Positive", "1:m dropped"),
        ("SWABS-6","ONS004","2020-08-09 00:00:00","Void", "1:m dropped"),
        ("SWABS-8","ONS006","2020-08-03 00:00:00","Positive", "m:1 dropped"),
        ("SWABS-12","ONS008","2020-08-04 00:00:00","Positive", "m:m dropped"),
        ("SWABS-15","ONS011","2020-08-04 00:00:00","Positive", "none:1"),
        # fmt: on
    ]
    expected_df_swab_residuals = spark_session.createDataFrame(
        expected_df_swab_residuals_data, schema=expected_df_swab_residuals_schema
    )

    expected_df_failed_records_schema = """
        unique_participant_response_id string,
        swab_sample_barcode string,
        visit_datetime string,
        iqvia_column string,
        unique_pcr_test_id string,
        pcr_result_recorded_datetime string,
        pcr_result_classification string,
        merge_type_info string
    """
    expected_df_failed_records_data = [
        # fmt: off
        ("IQVIA-13","ONS009","2020-08-01 01:00:00","i_data",     "SWABS-14","2020-08-03 00:00:00","Negative", "m:m failed"),
        ("IQVIA-13","ONS009","2020-08-01 01:00:00","i_data",     "SWABS-13","2020-08-02 00:00:00","Positive", "m:m failed"),
        ("IQVIA-14","ONS009","2020-08-02 01:00:00","i_data",     "SWABS-14","2020-08-03 00:00:00","Negative", "m:m failed"),
        ("IQVIA-14","ONS009","2020-08-02 01:00:00","i_data",     "SWABS-13","2020-08-02 00:00:00","Positive", "m:m failed"),
        # fmt: on
    ]
    expected_df_failed_records = spark_session.createDataFrame(
        expected_df_failed_records_data, schema=expected_df_failed_records_schema
    )

    df_voyager_swab, df_swab_residuals, df_failed_records = merge_swab(voyager_df, swab_df)

    assert_df_equality(expected_df_voyager_swab, df_voyager_swab, ignore_row_order=True, ignore_column_order=True)
    assert_df_equality(expected_df_swab_residuals, df_swab_residuals, ignore_row_order=True, ignore_column_order=True)
    assert_df_equality(expected_df_failed_records, df_failed_records, ignore_row_order=True, ignore_column_order=True)
