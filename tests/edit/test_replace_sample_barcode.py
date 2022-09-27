from chispa import assert_df_equality

from cishouseholds.edit import replace_sample_barcode


def test_replace_sample_barcode(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, "A", "B", 3, "Yes", "Yes", "C", "D"),
            (2, "A", "B", 3, "No", "No", "C", "D"),
            (3, "A", "B", 3, "Yes", "Yes", None, None),
            (4, "A", "B", 2, "Yes", "Yes", "C", "D"),
        ],
        schema="""
        id integer,
        swab_sample_barcode string,
        blood_sample_barcode string,
        survey_response_dataset_major_version integer,
        swab_sample_barcode_correct string,
        blood_sample_barcode_correct string,
        swab_sample_barcode_user_entered string,
        blood_sample_barcode_user_entered string
        """,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (1, "A", "B", 3, "Yes", "Yes", "C", "D", "A", "B"),
            (2, "A", "B", 3, "No", "No", "C", "D", "C", "D"),
            (3, "A", "B", 3, "Yes", "Yes", None, None, "A", "B"),
            (4, "A", "B", 2, "Yes", "Yes", "C", "D", "A", "B"),
        ],
        schema="""
        id integer,
        swab_sample_barcode string,
        blood_sample_barcode string,
        survey_response_dataset_major_version integer,
        swab_sample_barcode_correct string,
        blood_sample_barcode_correct string,
        swab_sample_barcode_user_entered string,
        blood_sample_barcode_user_entered string,
        swab_sample_barcode_combined string,
        blood_sample_barcode_combined string
        """,
    )

    output_df = replace_sample_barcode(input_df)
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_row_order=True)
