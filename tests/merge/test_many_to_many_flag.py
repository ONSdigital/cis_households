from chispa import assert_df_equality

from cishouseholds.merge import many_to_many_flag


def test_many_to_many_flag(spark_session):

    expected_df = spark_session.createDataFrame(
        data=[
            ("ABC123", None, 1, 0, 2, 2, 1, 1, "Positive", 1, None, 1),
            ("ABC123", None, 2, 1, 2, 2, 1, 2, "Positive", 1, 1, 1),
            ("ABC123", None, 9, 8, 2, 2, 2, 1, "Negative", 1, 1, 1),
            ("ABC123", None, 10, 9, 2, 2, 2, 2, "Positive", 1, None, 1),
            ("ABC456", None, 1, 0, 2, 2, 1, 1, "Positive", 1, None, None),
            ("ABC456", None, 2, 1, 2, 2, 1, 2, "Positive", 1, 1, None),
            ("ABC456", None, 9, 8, 2, 2, 2, 1, "Positive", 1, 1, None),
            ("ABC456", None, 10, 9, 2, 2, 2, 2, "Positive", 1, None, None),
            ("ABC789", None, 1, 0, 2, 2, 1, 1, "Negative", 1, None, None),
            ("ABC789", None, 1, 0, 2, 2, 2, 1, "Negative", 1, 1, None),
            ("ABC789", None, 1, 0, 2, 2, 3, 1, "Negative", 1, 1, None),
            ("ABC789", None, 5, 4, 2, 2, 1, 2, "Negative", 1, 1, None),
            ("ABC789", None, 5, 4, 2, 2, 2, 2, "Negative", 1, None, None),
            ("ABC789", None, 5, 4, 2, 2, 3, 2, "Negative", 1, 1, None),
            ("ABC789", None, 10, 9, 2, 2, 1, 3, "Negative", 1, 1, None),
            ("ABC789", None, 10, 9, 2, 2, 2, 3, "Negative", 1, 1, None),
            ("ABC789", None, 10, 9, 2, 2, 3, 3, "Negative", 1, None, None),
            ("DEF123", None, 1, 0, 2, 2, 1, 1, "Positive", 1, None, None),
            ("DEF123", None, 2, 1, 2, 2, 1, 2, "Positive", 1, 1, None),
            ("DEF123", 1, 9, 8, 2, 2, 2, 1, "Negative", None, None, None),
            ("DEF123", None, 10, 9, 2, 2, 2, 2, "Positive", 1, None, None),
            ("DEF789", 1, 1, 0, 2, 2, 1, 1, "Negative", None, None, None),
            ("DEF789", None, 1, 0, 2, 2, 2, 1, "Negative", 1, None, None),
            ("DEF789", None, 1, 0, 2, 2, 3, 1, "Negative", 1, 1, None),
            ("DEF789", None, 5, 4, 2, 2, 1, 2, "Negative", 1, None, None),
            ("DEF789", None, 5, 4, 2, 2, 2, 2, "Negative", 1, 1, None),
            ("DEF789", 1, 5, 4, 2, 2, 3, 2, "Negative", None, None, None),
            ("DEF789", None, 10, 9, 2, 2, 1, 3, "Negative", 1, 1, None),
            ("DEF789", None, 10, 9, 2, 2, 2, 3, "Negative", 1, 1, None),
        ],
        schema="antibody_barcode_cleaned string, out_of_date_range_antibody integer, diff_vs_visit integer, \
                abs_offset_diff_vs_visit integer, count_barcode_antibody integer, \
                count_barcode_voyager integer, unique_id_antibody integer, unique_id_voyager integer, \
                antibody_test_result_classification string, identify_mtom_flag integer, \
                drop_mtom_antibody_flag integer, failed_mtom_antibody_flag integer",
    )

    input_df = expected_df.drop(
        "identify_mtom_flag", "drop_mtom_antibody_flag", "failed_mtom_antibody_flag"
    )

    output_df = many_to_many_flag(
        input_df,
        "drop_mtom_antibody_flag",
        "antibody_barcode_cleaned",
        ["abs_offset_diff_vs_visit", "diff_vs_visit", "unique_id_voyager", "unique_id_antibody"],
        "antibody",
        "failed_mtom_antibody_flag",
    )

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
