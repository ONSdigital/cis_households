from chispa import assert_df_equality

from cishouseholds.edit import replace_sample_barcode


def test_replace_sample_barcode(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("A", "B", 3, "Yes", "Yes", "C", "D"),
            ("A", "B", 3, "No", "No", "C", "D"),
            ("A", "B", 3, "Yes", "Yes", None, None),
            ("A", "B", 2, "Yes", "Yes", "C", "D"),
        ],
        schema="""
        swab_barcode string,
        blood_barcode string,
        dataset_version integer,
        swab_barcode_correct string,
        blood_barcode_correct string,
        swab_barcode_entered string,
        blood_barcode_entered string
        """,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "B", "A", "B", 3, "Yes", "Yes", "C", "D"),
            ("C", "D", "A", "B", 3, "No", "No", "C", "D"),
            ("A", "B", "A", "B", 3, "No", "No", None, None),
            ("A", "B", "A", "B", 2, "Yes", "Yes", "C", "D"),
        ],
        schema="""
        swab_barcode_combined string,
        blood_barcode_combined string,
        swab_barcode string,
        blood_barcode string,
        dataset_version integer,
        swab_barcode_correct string,
        blood_barcode_correct string,
        swab_barcode_entered string,
        blood_barcode_entered string
        """,
    )

    output_df = replace_sample_barcode(
        input_df,
        "swab_barcode_combined",
        "blood_barcode_combined",
        "swab_barcode",
        "blood_barcode",
        "dataset_version",
        "swab_barcode_correct",
        "blood_barcode_correct",
        "swab_barcode_entered",
        "blood_barcode_entered",
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
