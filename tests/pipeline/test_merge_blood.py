from chispa import assert_df_equality

from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_blood


def test_merge_blood(spark_session):
    voyager_schema = """
    unique_participant_response_id string,
    blood_sample_barcode string,
    visit_date_string string,
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
        # fmt: on
    ]
    voyager_df = spark_session.createDataFrame(voyager_data, schema=voyager_schema)

    bloods_schema = """
    unique_antibody_test_id string,
    blood_sample_barcode string,
    blood_sample_received_date_s_protein string,
    antibody_test_result_recorded_date_s_protein string,
    antibody_test_result_classification_s_protein string,
    siemens_antibody_test_result_value_s_protein string,
    antibody_test_result_value_s_protein integer
    """
    bloods_data = [
        # fmt: off
        ("BLOODS-1","ONS001","2020-08-02",None,"Positive",None,10),
        ("BLOODS-2","ONS002","2020-08-12",None,"Negative",None,10),
        ("BLOODS-3","ONS003","2020-08-03",None,"Positive",None,10),
        ("BLOODS-4","ONS003","2020-08-04",None,"Positive",None,10),
        ("BLOODS-5","ONS004","2020-08-11",None,"Negative",None,10),
        ("BLOODS-6","ONS004","2020-08-11",None,"Negative",None,12),
        ("BLOODS-7","ONS005","2020-08-02",None,"Positive",None,10),
        ("BLOODS-8","ONS006","2020-08-03",None,"Void",None,10),
        ("BLOODS-9","ONS007","2020-08-06",None,"Negative",None,10),
        ("BLOODS-10","ONS007","2020-08-08",None,"Negative",None,10),
        ("BLOODS-11","ONS008","2020-08-03",None,"Positive",None,10),
        ("BLOODS-12","ONS008","2020-08-04",None,"Positive",None,10),
        ("BLOODS-13","ONS009","2020-08-02",None,"Negative",None,10),
        ("BLOODS-14","ONS009","2020-08-03",None,"Positive",None,10),
        ("BLOODS-15","ONS010","2020-08-02",None,"Positive",None,10),
        ("BLOODS-16","ONS010","2020-08-03",None,"Negative",None,10),
        # fmt: on
    ]
    bloods_df = spark_session.createDataFrame(bloods_data, schema=bloods_schema)

    expected_voyager_bloods_schema = """
        unique_participant_response_id string,
        blood_sample_barcode string,
        visit_date_string string,
        iqvia_column string,
        unique_antibody_test_id string,
        blood_sample_received_date_s_protein string,
        antibody_test_result_recorded_date_s_protein string,
        antibody_test_result_classification_s_protein string,
        siemens_antibody_test_result_value_s_protein string,
        antibody_test_result_value_s_protein integer
    """
    expected_voyager_bloods_data = [
        # fmt: off
        ("IQVIA-1","ONS001","2020-08-01","i_data",      "BLOODS-1","2020-08-02",None,"Positive",None,10),
        ("IQVIA-2","ONS002","2020-08-01","i_data",      None, None, None, None, None, None),
        ("IQVIA-3","ONS003","2020-08-01","i_data",      "BLOODS-3","2020-08-03",None,"Positive",None,10),
        ("IQVIA-4","ONS004","2020-08-01","i_data",      "BLOODS-5","2020-08-11",None,"Negative",None,10),
        ("IQVIA-5","ONS005","2020-08-01","i_data",      "BLOODS-7","2020-08-02",None,"Positive",None,10),
        ("IQVIA-6","ONS005","2020-08-04","i_data",      None, None, None, None, None, None),
        ("IQVIA-7","ONS006","2020-08-01","i_data",      None, None, None, None, None, None),
        ("IQVIA-8","ONS006","2020-08-02","i_data",      None, None, None, None, None, None),
        ("IQVIA-9","ONS007","2020-08-01","i_data",      "BLOODS-9","2020-08-06",None,"Negative",None,10),
        ("IQVIA-10","ONS007","2020-08-02","i_data",     "BLOODS-10","2020-08-08",None,"Negative",None,10),
        ("IQVIA-11","ONS008","2020-08-01","i_data",     "BLOODS-11","2020-08-03",None,"Positive",None,10),
        ("IQVIA-12","ONS008","2020-08-07","i_data",     None, None, None, None, None, None),
        ("IQVIA-13","ONS009","2020-08-01","i_data",     None, None, None, None, None, None),
        ("IQVIA-14","ONS010","2020-08-01","i_data",     None, None, None, None, None, None),
        ("IQVIA-15","ONS010","2020-08-02","i_data",     None, None, None, None, None, None),
        # fmt: on
    ]
    expected_df_voyager_bloods = spark_session.createDataFrame(
        expected_voyager_bloods_data, schema=expected_voyager_bloods_schema
    )

    df_voyager_bloods, df_bloods_residuals, df_failed_records = merge_blood(voyager_df, bloods_df)

    assert_df_equality(expected_df_voyager_bloods, df_voyager_bloods, ignore_row_order=True, ignore_column_order=True)
