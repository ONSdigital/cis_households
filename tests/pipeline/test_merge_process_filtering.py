from chispa import assert_df_equality

from cishouseholds.pipeline.merge_process import merge_process_filtering


def test_merge_process_filtering(spark_session):
    schema = """barcode string, 1tom_swab string, drop_flag_1tom_swab string,
                failed_1tom_swab string, anything string"""
    data = [
        ("ONS001", 1, None, None, "anything"),  # Best match 1tom
        ("ONS001", 1, 1, None, "anything"),  # Not Best match
        ("ONS001", 1, 1, 1, "anything"),  # Not Best match
        # ('ONS001',  1,  None,   None,   'anything'), # Failed to match
    ]
    df_input = spark_session.createDataFrame(data, schema=schema)

    schema = """barcode string, 1tom_swab string, drop_flag_1tom_swab string,
                failed_1tom_swab string"""

    data = [("ONS001", 1, None, None)]  # Best match 1tom
    df_expected_success_match = spark_session.createDataFrame(data, schema=schema)

    data = [("ONS001", 1, 1, None)]  # Not Best match
    df_expected_unsuccess_match = spark_session.createDataFrame(data, schema=schema)

    data = [("ONS001", 1, 1, 1)]  # Failed match
    df_expected_failed = spark_session.createDataFrame(data, schema=schema)

    df_output_success, df_output_unsuccess, df_output_fail = merge_process_filtering(
        df=df_input,
        merge_type="swab",
        lab_columns_list=[],
        merge_combination=["1tom"],
        drop_list_columns=["anything"],
    )
    assert_df_equality(df_output_success, df_expected_success_match)
    assert_df_equality(df_output_unsuccess, df_expected_unsuccess_match)
    assert_df_equality(df_output_fail, df_expected_failed)
