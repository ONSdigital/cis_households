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
        swab_sample_barcode string,
        blood_sample_barcode string,
        dataset_version integer,
        swab_sample_barcode_correct string,
        blood_sample_barcode_correct string,
        swab_sample_barcode_user_entered string,
        blood_sample_barcode_user_entered string
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
        swab_sample_barcode_combined string,
        blood_sample_barcode_combined string,
        swab_sample_barcode string,
        blood_sample_barcode string,
        dataset_version integer,
        swab_sample_barcode_correct string,
        blood_sample_barcode_correct string,
        swab_sample_barcode_user_entered string,
        blood_sample_barcode_user_entered string
        """,
    )

    output_df = replace_sample_barcode(
        input_df,
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
