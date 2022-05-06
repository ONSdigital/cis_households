from chispa import assert_df_equality

from cishouseholds.merge import assign_merge_process_group_flag


def test_assign_merge_process_group_flag(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(1, 1, 1, 2, None), (2, None, 2, 1, 1), (3, 1, 2, 1, 1), (4, None, 1, 1, None), (5, None, 1, 2, None)],
        schema="""id integer, out_of_date_range_flag integer, count_barcode_labs integer,
        count_barcode_voyager integer, merge_process_group_flag integer""",
    )

    input_df = expected_df.drop("merge_process_group_flag")

    output_df = assign_merge_process_group_flag(
        input_df,
        column_name_to_assign="merge_process_group_flag",
        out_of_date_range_flag="out_of_date_range_flag",
        count_barcode_labs_column_name="count_barcode_labs",
        count_barcode_labs_condition=">1",
        count_barcode_voyager_column_name="count_barcode_voyager",
        count_barcode_voyager_condition="==1",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
