from chispa import assert_df_equality

from cishouseholds.edit import replace_sample_barcode


def test_replace_sample_barcode(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("A", "B", "C", "D", 3, "Yes", "Yes", "E", "F"),
            ("A", "B", "C", "D", 3, "No", "No", "E", "F"),
            ("A", "B", "C", "D", 3, "Yes", "Yes", None, None),
            ("A", "B", "C", "D", 2, "Yes", "Yes", "E", "F"),
        ],
        schema="""
        swab_barcode_combined string,
        blood_barcode_combined string,
        swab_barcode string,
        blood_barcode string
        dataset_version integer,
        swab_barcode_correct string,
        blood_barcode_correct string
        swab_barcode_entered string,
        blood_barcode_entered string
        """,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "B", "C", "D", 3, "Yes", "Yes", "E", "F"),
            ("E", "F", "C", "D", 3, "No", "No", "E", "F"),
            ("C", "D", "C", "D", 3, "Yes", "Yes", None, None),
            ("C", "D", "C", "D", 2, "Yes", "Yes", "E", "F"),
        ],
        schema="""
        swab_barcode_combined string,
        blood_barcode_combined string,
        swab_barcode string,
        blood_barcode string
        dataset_version integer,
        swab_barcode_correct string,
        blood_barcode_correct string
        swab_barcode_entered string,
        blood_barcode_entered string
        """,
    )

    output_df = replace_sample_barcode(
        input_df, "swab_barcode_combined", "blood_barcode_combined", "swab_barcode", "blood_barcode"
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
