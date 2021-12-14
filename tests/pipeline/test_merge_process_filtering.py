from chispa import assert_df_equality

from cishouseholds.pipeline.merge_process import merge_process_filtering


def test_merge_process_filtering(spark_session):
    schema = """
                iqvia_col string,
                lab_col string,
                barcode string,
                unique_participant_response_id integer,
                unique_pcr_test_id integer,
                out_of_date_range_swab integer,

                1to1_swab integer,
                1tonone_swab integer,
                noneto1_swab integer,

                1tom_swab integer,
                mto1_swab integer,
                mtom_swab integer,
                drop_flag_1tom_swab integer,
                drop_flag_mto1_swab integer,
                drop_flag_mtom_swab integer,
                failed_1tom_swab integer,
                failed_mto1_swab integer,
                failed_mtom_swab integer,
                failed_flag_mtom_swab integer,

                best_match integer,
                not_best_match integer,
                failed_match integer
            """
    data = [
        # fmt: off
        # A is 1:1 matching
        ("I", "L", "A1", 1, 1, None, 1, None, None, None, None, None, None, None, None, None, None, None, None, 1, None, None),
        ("I", "L", "A2", 2, 2, 1, 1, None, None, None, None, None, None, None, None, None, None, None, None, None, 1, None),
        # B is 1:m matching
        ("I", "L", "B1", 3, 3, None, None, None, None, 1, None, None, 1, None, None, None, None, None, None, None, 1, None),
        ("I", "L", "B1", 3, 4, None, None, None, None, 1, None, None, None, None, None, None, None, None, None, 1, None, None),
        ("I", "L", "B2", 4, 5, None, None, None, None, 1, None, None, None, None, None, None, None, None, None, 1, None, None),
        ("I", "L", "B2", 4, 6, 1, None, None, None, 1, None, None, None, None, None, None, None, None, None, None, 1, None),
        ("I", "L", "B3", 5, 7, None, None, None, None, 1, None, None, None, None, None, 1, None, None, None, None, None, 1),
        ("I", "L", "B3", 5, 8, None, None, None, None, 1, None, None, None, None, None, 1, None, None, None, None, None, 1),
        # C is m:1 matching
        ("I", "L", "C1", 6, 9, None, None, None, None, None, 1, None, None, None, None, None, None, None, None, 1, None, None),
        ("I", "L", "C1", 7, 9, None, None, None, None, None, 1, None, None, 1, None, None, None, None, None, None, 1, None),
        ("I", "L", "C2", 8, 10, 1, None, None, None, None, 1, None, None, None, None, None, None, None, None, None, 1, None),
        ("I", "L", "C2", 9, 10, None, None, None, None, None, 1, None, None, None, None, None, None, None, None, 1, None, None),
        ("I", "L", "C3", 10, 11, None, None, None, None, None, 1, None, None, None, None, None, 1, None, None, None, None, 1),
        ("I", "L", "C3", 11, 11, None, None, None, None, None, 1, None, None, None, None, None, 1, None, None, None, None, 1),
        # D is m:m matching
        ("I", "L", "D1", 12, 12, 1, None, None, None, None, None, 1, None, None, None, None, None, None, None, None, 1, None),
        ("I", "L", "D1", 13, 12, None, None, None, None, None, None, 1, None, None, None, None, None, None, None, 1, None, None),
        ("I", "L", "D1", 12, 13, None, None, None, None, None, None, 1, None, None, None, None, None, None, None, 1, None, None),
        ("I", "L", "D1", 13, 13, None, None, None, None, None, None, 1, None, None, 1, None, None, None, None, None, 1, None),
        ("I", "L", "D2", 14, 14, None, None, None, None, None, None, 1, None, None, None, None, None, None, 1, None, None, 1),
        ("I", "L", "D2", 15, 14, None, None, None, None, None, None, 1, None, None, None, None, None, None, 1, None, None, 1),
        ("I", "L", "D2", 14, 15, None, None, None, None, None, None, 1, None, None, None, None, None, 1, None, None, None, 1),
        ("I", "L", "D2", 15, 15, None, None, None, None, None, None, 1, None, None, None, None, None, 1, None, None, None, 1),
        ("I", "L", "D3", 16, 16, 1, None, None, None, None, None, 1, None, None, None, None, None, None, None, None, 1, None),
        ("I", "L", "D3", 17, 16, 1, None, None, None,None, None, 1, None, None, None, None, None, None, None, None, 1, None),
        ("I", "L", "D3", 16, 17, None, None, None, None, None, None, 1, None, None, None, None, None, None, None, 1, None, None),
        ("I", "L", "D3", 17, 17, None, None, None, None, None, None, 1, None, None, 1, None, None, None, None, None, 1, None),
        # E is extra/specific cases
        ("I", "L", "E1", 18, 18, 1, None, None, None, 1, None, None, None, None, None, 1, None, None, None, None, None, 1),
        ("I", "L", "E1", 19, 18, None, None, None, None, 1, None, None, 1, None, None, 1, None, None, None, None, None, 1),
        # fmt: on
    ]

    data_none_record = [
        # fmt: off
        # F one to None and None to one cases
        ("I", None, "F1", 20, None, None, None, 1, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
        (None, "L", "F2", None, 21, None, None, None, 1, None, None, None, None, None, None, None, None, None, None, None, None, None),
        # fmt: on
    ]

    df_input = spark_session.createDataFrame(data, schema=schema)
    df_input = df_input.drop("best_match", "not_best_match", "failed_match")
    none_record_df = spark_session.createDataFrame(data_none_record, schema=schema).drop(
        "best_match", "not_best_match", "failed_match"
    )

    schema_iqvia = """
        iqvia_col string,
        lab_col string,
        barcode string,
        unique_participant_response_id integer,
        unique_pcr_test_id integer
    """
    data_all_iqvia = [
        ("I", "L", "A1", 1, 1),
        ("I", None, "A2", 2, None),
        ("I", "L", "B1", 3, 4),
        ("I", "L", "B2", 4, 5),
        ("I", None, "B3", 5, None),
        ("I", "L", "C1", 6, 9),
        ("I", None, "C1", 7, None),
        ("I", None, "C2", 8, None),
        ("I", "L", "C2", 9, 10),
        ("I", None, "C3", 10, None),
        ("I", None, "C3", 11, None),
        ("I", "L", "D1", 13, 12),
        ("I", "L", "D1", 12, 13),
        ("I", None, "D2", 14, None),
        ("I", None, "D2", 15, None),
        ("I", None, "D3", 17, None),
        ("I", "L", "D3", 16, 17),
        ("I", None, "E1", 18, None),
        ("I", None, "E1", 19, None),
        ("I", None, "F1", 20, None),
    ]
    df_all_iqvia_expected = spark_session.createDataFrame(data_all_iqvia, schema=schema_iqvia)

    schema_labs = """
        lab_col string,
        barcode string,
        unique_pcr_test_id integer
    """
    data_labs = [("L", "A2", 2), ("L", "B1", 3), ("L", "B2", 6), ("L", "D3", 16), ("L", "F2", 21)]
    df_residuals_expected = spark_session.createDataFrame(data_labs, schema=schema_labs)

    data_failed = [
        ("I", "L", "B3", 5, 7),
        ("I", "L", "B3", 5, 8),
        ("I", "L", "C3", 10, 11),
        ("I", "L", "C3", 11, 11),
        ("I", "L", "D2", 14, 14),
        ("I", "L", "D2", 15, 14),
        ("I", "L", "D2", 14, 15),
        ("I", "L", "D2", 15, 15),
        ("I", "L", "E1", 18, 18),
        ("I", "L", "E1", 19, 18),
    ]
    df_failed_expected = spark_session.createDataFrame(data_failed, schema=schema_iqvia)

    df_all_iqvia, df_residuals, df_failed = merge_process_filtering(
        df=df_input,
        none_record_df=none_record_df,
        merge_type="swab",
        barcode_column_name="barcode",
        lab_columns_list=["lab_col", "unique_pcr_test_id"],
        merge_combination=["1tom", "mto1", "mtom"],
    )
    assert_df_equality(df_all_iqvia_expected, df_all_iqvia, ignore_row_order=True, ignore_column_order=True)
    assert_df_equality(df_residuals_expected, df_residuals, ignore_row_order=True, ignore_column_order=True)
    assert_df_equality(df_failed_expected, df_failed, ignore_row_order=True, ignore_column_order=True)
