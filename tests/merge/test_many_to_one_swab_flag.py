from chispa import assert_df_equality

from cishouseholds.merge import many_to_one_swab_flag


def test_many_to_one_swab_flag(spark_session):

    expected_df = spark_session.createDataFrame(
        data=[
            ("ABC123", None, 24, 0, 1, 2, 1),
            ("ABC123", None, 28, 4, 1, 2, 1),
            ("ABC456", None, 48, 24, 1, 2, 1),
            ("ABC456", None, 48, 24, 1, 2, 1),
            ("ABC789", None, 28, 4, 1, 2, None),
            ("ABC789", None, 36, 12, 1, 2, 1),
            ("DEF123", None, 25, 1, 1, 3, None),
            ("DEF123", None, 35, 11, 1, 3, 1),
            ("DEF123", None, 38, 14, 1, 3, 1),
            ("DEF456", None, -1, 25, 1, 2, None),
            ("DEF456", None, 57, 33, 1, 2, 1),
            ("DEF789", None, 7, 17, 1, 2, None),
            ("DEF789", None, 41, 17, 1, 2, 1),
            ("GHI123", None, 20, 4, 1, 2, None),
            ("GHI123", None, 37, 13, 1, 2, 1),
            ("GHI456", 1, 20, 4, 1, 2, None),
            ("GHI456", 1, 37, 13, 1, 2, None),
            ("GHI789", None, 25, 1, 1, 3, None),
            ("GHI789", None, 35, 11, 1, 3, 1),
            ("GHI789", 1, 38, 14, 1, 3, None),
        ],
        schema="swab_barcode_cleaned string, out_of_date_range_swab integer, diff_vs_visit_hr integer, \
                abs_offset_diff_vs_visit_hr integer, count_barcode_swab integer, \
                count_barcode_voyager integer, drop_many_to_one_swab_flag integer",
    )

    input_df = expected_df.drop("drop_many_to_one_swab_flag")

    output_df = many_to_one_swab_flag(
        input_df,
        "drop_many_to_one_swab_flag",
        "swab_barcode_cleaned",
        ["abs_offset_diff_vs_visit_hr", "diff_vs_visit_hr"],
    )

    assert_df_equality(output_df, expected_df, ignore_row_order=True)
