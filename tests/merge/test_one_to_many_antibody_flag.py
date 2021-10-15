from chispa import assert_df_equality

from cishouseholds.merge import one_to_many_antibody_flag


def test_one_to_many_antibody_flag(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2029-01-01", "ONS00000003", "negative", "ONS00000003", "2029-01-01", 4, "positive", 0.0, None, 1, 1),
            (1, "2029-01-01", "ONS00000003", "positive", "ONS00000003", "2029-01-01", 4, "positive", 0.0, None, 1, 1),
            (1, "2029-01-06", "ONS00000005", "positive", "ONS00000005", "2029-01-05", 3, "positive", -24.0, None, 1, 1),
            (
                1,
                "2029-01-06",
                "ONS00000005",
                "positive",
                "ONS00000005",
                "2029-01-06",
                3,
                "positive",
                0.0,
                None,
                1,
                None,
            ),
            (
                1,
                "2029-01-04",
                "ONS00000007",
                "positive",
                "ONS00000007",
                "2029-01-05",
                3,
                "positive",
                24.0,
                None,
                1,
                None,
            ),
            (
                1,
                "2029-01-04",
                "ONS00000007",
                "positive",
                "ONS00000007",
                "2029-01-05",
                3,
                "positive",
                24.0,
                None,
                1,
                None,
            ),
            (
                1,
                "2029-01-04",
                "ONS00000006",
                "negative",
                "ONS00000006",
                "2029-01-05",
                3,
                "negative",
                24.0,
                None,
                1,
                None,
            ),
            (
                2,
                "2029-01-02",
                "ONS00000004",
                "negative",
                "ONS00000004",
                "2029-01-02",
                2,
                "negative",
                0.0,
                None,
                None,
                None,
            ),
            (2, "2029-01-02", "ONS00000004", "negative", "ONS00000004", "2029-01-05", 2, None, 72.0, 1, None, None),
            (
                1,
                "2029-01-02",
                "ONS00000004",
                "negative",
                "ONS00000004",
                "2029-01-02",
                2,
                "negative",
                0.0,
                None,
                1,
                None,
            ),
            (1, "2029-01-01", "ONS00000004", "negative", "ONS00000004", "2029-01-02", 2, "negative", 24.0, None, 1, 1),
            (1, "2029-01-02", "ONS00000004", "negative", "ONS00000004", "2029-01-03", 2, None, 24.0, None, 1, 1),
            (1, "2029-01-01", "ONS00000004", "negative", "ONS00000004", "2029-01-03", 2, None, 48.0, None, 1, 1),
        ],
        schema="count_barcode_voyager integer, visit_date string, barcode_iq string, tdi string, barcode_ox string, \
                received_ox_date string, count_barcode_antibody integer, siemens string,\
                diff_interval_hours double, out_of_date_range_antibody integer, \
                identify_one_to_many_antibody_flag integer, one_to_many_antibody_drop_flag integer",
    )

    input_df = expected_df.drop(
        "identify_one_to_many_antibody_flag", "one_to_many_antibody_drop_flag", "failed_due_to_indistinct_match"
    )

    output_df = one_to_many_antibody_flag(
        df=input_df,
        column_name_to_assign="one_to_many_antibody_drop_flag",
        group_by_column="barcode_iq",
        diff_interval_hours="diff_interval_hours",
        siemens_column="siemens",
        tdi_column="tdi",
        visit_date="visit_date",
        out_of_date_range_column="out_of_date_range_antibody",
        count_barcode_voyager_column_name="count_barcode_voyager",
        count_barcode_labs_column_name="count_barcode_antibody",
    )
    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
