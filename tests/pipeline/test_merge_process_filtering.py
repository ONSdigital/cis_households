import pytest
from chispa import assert_df_equality

# from cishouseholds.pipeline.merge_process import merge_process_filtering


@pytest.mark.xfail(reason="units do not function correctly")
def merge_process_filtering(spark_session):
    schema = """
                barcode string,

                date_visit string,
                date_received string,
                pcr_result_recorded_datetime string,
                pcr_result_classification string,
                out_of_date_range_swab integer,
                arbitrary_lab integer,
                unique_participant_response_id integer,
                unique_pcr_test_id integer,
                drop_flag_1tom_swab integer,
                drop_flag_mto1_swab integer,
                failed_flag_mtom_swab integer,
                drop_flag_mtom_swab integer,
                1tom_swab integer,
                mto1_swab integer,
                mtom_swab integer,
                failed_1tom_swab integer,
                failed_mto1_swab integer,
                failed_mtom_swab integer
            """
    data = [
        ("ONS0003", "A", "B", "C", "D", None, 0, 3, 5, None, 1, None, None, 1, None, None, None, None, None),
        ("ONS0003", "A", "B", "C", "D", None, 0, 4, 5, 1, 1, None, 1, 1, None, None, None, None, None),
        ("ONS0003", "A", "B", "C", "D", None, 0, 5, 5, 1, 1, None, 1, 1, None, None, 1, None, None),
        ("ONS0004", "A", "B", "C", "D", None, 0, 8, 7, 1, None, 1, None, None, None, 1, None, None, None),
        ("ONS0004", "A", "B", "C", "D", None, 0, 6, 7, 1, None, 1, None, None, None, 1, None, None, None),
        ("ONS0004", "A", "B", "C", "D", None, 0, 6, 8, 1, None, None, None, None, None, 1, None, None, None),
        ("ONS0004", "A", "B", "C", "D", None, 0, 7, 8, 1, None, 1, 1, None, None, 1, None, None, None),
        ("ONS0004", "A", "B", "C", "D", None, 0, 8, 8, 1, None, None, None, None, None, 1, None, None, 1),
        ("ONS0004", "A", "B", "C", "D", None, 0, 7, 7, 1, None, None, None, None, None, 1, None, None, 1),
        ("ONS0004", "A", "B", "C", "D", None, 0, 6, 6, 1, None, 1, None, None, None, 1, None, None, 1),
        ("ONS0004", "A", "B", "C", "D", None, 0, 7, 6, 1, None, None, 1, None, None, 1, None, None, 1),
        ("ONS0004", "A", "B", "C", "D", None, 0, 8, 6, 1, None, None, 1, None, None, 1, None, None, 1),
        ("ONS0002", "A", "B", "C", "D", None, 0, 2, 3, None, None, None, 1, None, 1, None, None, None, None),
        ("ONS0002", "A", "B", "C", "D", None, 0, 2, 2, None, 1, None, None, None, 1, None, None, None, None),
        ("ONS0002", "A", "B", "C", "D", None, 0, 2, 4, None, None, None, 1, None, 1, None, None, 1, None),
        ("ONS0001", "A", "B", "C", "D", 1, 0, 1, 1, 1, None, None, None, None, None, None, None, None, None),
        ("ONS0000", "A", "B", "C", "D", None, 0, 1, 1, 1, None, None, None, None, None, None, None, None, None),
    ]
    df_input = spark_session.createDataFrame(data, schema=schema)

    df_output_all_iqvia, df_lab_residuals, df_failed_records = merge_process_filtering(
        df=df_input,
        merge_type="swab",
        barcode_column_name="barcode",
        lab_columns_list=["date_visit", "date_received", "pcr_result_recorded_datetime", "pcr_result_classification"],
        merge_combination=["1tom", "mto1", "mtom"],
        drop_list_columns=["arbitrary_lab"],
    )

    assert_df_equality(
        df_input, df_output_all_iqvia, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True
    )
