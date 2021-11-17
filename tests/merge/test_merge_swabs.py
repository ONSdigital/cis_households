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
        ("IQVIA-1","ONS001","2020-08-01","i_data"),
        ("IQVIA-2","ONS002","2020-08-01","i_data"),
        ("IQVIA-3","ONS003","2020-08-01","i_data"),
        ("IQVIA-4","ONS004","2020-08-01","i_data"),
        ("IQVIA-5","ONS005","2020-08-01","i_data"),
        ("IQVIA-6","ONS005","2020-08-04","i_data"),
        ("IQVIA-7","ONS006","2020-08-01","i_data"),
        ("IQVIA-8","ONS006","2020-08-02","i_data"),
        ("IQVIA-9","ONS007","2020-08-01","i_data"),
        ("IQVIA-10","ONS007","2020-08-02","i_data"),
        ("IQVIA-11","ONS008","2020-08-01","i_data"),
        ("IQVIA-12","ONS008","2020-08-07","i_data"),
        ("IQVIA-13","ONS009","2020-08-01","i_data"),
        ("IQVIA-14","ONS010","2020-08-01","i_data"),
        ("IQVIA-15","ONS010","2020-08-02","i_data"),
        ("IQVIA-16","ONS011","2020-08-01","i_data"),
        # fmt: on
    ]
    voyager_df = spark_session.createDataFrame(voyager_data, schema=voyager_schema)

    SWABS_schema = """
        unique_pcr_test_id string,
        swab_sample_barcode string,
        swab_sample_received_date string,
        pcr_datetime string,
        pcr_result_classification string,
        merge_type_info string
    """
    SWABS_data = [
        # fmt: off
        ("SWABS-1","ONS001","2020-08-02",None,"Positive", "1:1 success"),
        ("SWABS-2","ONS002","2020-08-12",None,"Negative", "1:1 out-of-range"),
        ("SWABS-3","ONS003","2020-08-03",None,"Positive", "1:m success"),
        ("SWABS-4","ONS003","2020-08-04",None,"Positive", "1:m dropped"),
        ("SWABS-5","ONS004","2020-08-11",None,"Negative", "1:m success"), #exact duplicate to test random priority of first result
        ("SWABS-6","ONS004","2020-08-11",None,"Negative", "1:m dropped"),
        ("SWABS-7","ONS005","2020-08-02",None,"Positive", "m:1 success"),
        ("SWABS-8","ONS006","2020-08-03",None,"Void", "m:1 dropped"),
        ("SWABS-9","ONS007","2020-08-06",None,"Negative", "m:m success"),
        ("SWABS-10","ONS007","2020-08-08",None,"Negative", "m:m success"),
        ("SWABS-11","ONS008","2020-08-03",None,"Positive", "m:m success"),
        ("SWABS-12","ONS008","2020-08-04",None,"Positive", "m:m dropped"),
        ("SWABS-13","ONS009","2020-08-02",None,"Negative", "1:m failed"),
        ("SWABS-14","ONS009","2020-08-03",None,"Positive", "1:m failed"),
        ("SWABS-15","ONS010","2020-08-02",None,"Positive", "m:m failed"),
        ("SWABS-16","ONS010","2020-08-03",None,"Negative", "m:m failed"),
        ("SWABS-17","ONS012","2020-08-04",None,"Positive", "none:1"),
        # fmt: on
    ]
    SWABS_df = spark_session.createDataFrame(SWABS_data, schema=SWABS_schema)

    expected_voyager_SWABS_schema = """
        unique_participant_response_id string,
        swab_sample_barcode string,
        visit_datetime string,
        iqvia_column string,
        unique_pcr_test_id string,
        swab_sample_received_date string,
        pcr_datetime string,
        pcr_result_classification string,
        merge_type_info string
    """
    expected_voyager_SWABS_data = [
        # fmt: off
        ("IQVIA-1","ONS001","2020-08-01","i_data",      "SWABS-1","2020-08-02",None,"Positive", "1:1 success"),
        ("IQVIA-2","ONS002","2020-08-01","i_data",      None, None, None, None, None),
        ("IQVIA-3","ONS003","2020-08-01","i_data",      "SWABS-3","2020-08-03",None,"Positive", "1:m success"),
        ("IQVIA-4","ONS004","2020-08-01","i_data",      "SWABS-5","2020-08-11",None,"Negative", "1:m success"),
        ("IQVIA-5","ONS005","2020-08-01","i_data",      "SWABS-7","2020-08-02",None,"Positive", "m:1 success"),
        ("IQVIA-6","ONS005","2020-08-04","i_data",      None, None, None, None, None),
        ("IQVIA-7","ONS006","2020-08-01","i_data",      None, None, None, None, None),
        ("IQVIA-8","ONS006","2020-08-02","i_data",      None, None, None, None, None),
        ("IQVIA-9","ONS007","2020-08-01","i_data",      "SWABS-10","2020-08-08",None,"Negative", "m:m success"),
        ("IQVIA-10","ONS007","2020-08-02","i_data",     "SWABS-9","2020-08-06",None,"Negative", "m:m success"),
        ("IQVIA-11","ONS008","2020-08-01","i_data",     "SWABS-11","2020-08-03",None,"Positive", "m:m success"),
        ("IQVIA-12","ONS008","2020-08-07","i_data",     None, None, None, None, None),
        ("IQVIA-13","ONS009","2020-08-01","i_data",     None, None, None, None, None),
        ("IQVIA-14","ONS010","2020-08-01","i_data",     None, None, None, None, None),
        ("IQVIA-15","ONS010","2020-08-02","i_data",     None, None, None, None, None),
        ("IQVIA-16","ONS011","2020-08-01","i_data",     None, None, None, None, None)
        # fmt: on
    ]
    expected_df_voyager_SWABS = spark_session.createDataFrame(
        expected_voyager_SWABS_data, schema=expected_voyager_SWABS_schema
    )

    expected_df_SWABS_residuals_schema = """
        unique_pcr_test_id string,
        swab_sample_barcode string,
        swab_sample_received_date string,
        pcr_datetime string,
        pcr_result_classification string,
        merge_type_info string
    """
    expected_df_SWABS_residuals_data = [
        # fmt: off
        ("SWABS-2","ONS002","2020-08-12",None,"Negative", "1:1 out-of-range"),
        ("SWABS-4","ONS003","2020-08-04",None,"Positive", "1:m dropped"),
        ("SWABS-6","ONS004","2020-08-11",None,"Negative", "1:m dropped"),
        ("SWABS-8","ONS006","2020-08-03",None,"Void", "m:1 dropped"),
        ("SWABS-12","ONS008","2020-08-04",None,"Positive", "m:m dropped"),
        ("SWABS-17","ONS012","2020-08-04",None,"Positive", "none:1"),
        # fmt: on
    ]
    expected_df_SWABS_residuals = spark_session.createDataFrame(
        expected_df_SWABS_residuals_data, schema=expected_df_SWABS_residuals_schema
    )

    expected_df_failed_records_schema = """
        unique_participant_response_id string,
        swab_sample_barcode string,
        visit_datetime string,
        iqvia_column string,
        unique_pcr_test_id string,
        swab_sample_received_date string,
        pcr_datetime string,
        pcr_result_classification string,
        merge_type_info string
    """
    expected_df_failed_records_data = [
        # fmt: off
        ("IQVIA-13","ONS009","2020-08-01","i_data",     "SWABS-13","2020-08-02",None,"Negative", "1:m failed"),
        ("IQVIA-13","ONS009","2020-08-01","i_data",     "SWABS-14","2020-08-03",None,"Positive", "1:m failed"),
        ("IQVIA-14","ONS010","2020-08-01","i_data",     "SWABS-15","2020-08-02",None,"Positive", "m:m failed"),
        ("IQVIA-14","ONS010","2020-08-01","i_data",     "SWABS-16","2020-08-03",None,"Negative", "m:m failed"),
        ("IQVIA-15","ONS010","2020-08-02","i_data",     "SWABS-15","2020-08-02",None,"Positive", "m:m failed"),
        ("IQVIA-15","ONS010","2020-08-02","i_data",     "SWABS-16","2020-08-03",None,"Negative", "m:m failed"),
        # fmt: on
    ]
    expected_df_failed_records = spark_session.createDataFrame(
        expected_df_failed_records_data, schema=expected_df_failed_records_schema
    )

    df_voyager_SWABS, df_SWABS_residuals, df_failed_records = merge_swab(voyager_df, SWABS_df)

    assert_df_equality(expected_df_voyager_SWABS, df_voyager_SWABS, ignore_row_order=True, ignore_column_order=True)
    # assert_df_equality(
    #     expected_df_SWABS_residuals, df_SWABS_residuals, ignore_row_order=True, ignore_column_order=True
    # )
    # assert_df_equality(expected_df_failed_records, df_failed_records, ignore_row_order=True, ignore_column_order=True)
