from chispa import assert_df_equality

from cishouseholds.edit import replace_sample_barcode


def test_replace_sample_barcode(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (
                1,
                "2022-12-01 00:00:00",
                "A",
                "B",
                3,
                "Yes",
                "Yes",
                None,
                None,
                "C",
                "D",
            ),  # Tests to preserve preassigned A and B in new col
            (
                2,
                "2022-12-01 00:00:00",
                "A",
                "B",
                3,
                "No",
                "No",
                None,
                None,
                "C",
                "D",
            ),  # Tests to replace A and B with C and D in new col
            (
                3,
                "2022-12-01 00:00:00",
                "A",
                "B",
                3,
                "Yes",
                "Yes",
                None,
                None,
                None,
                None,
            ),  # Tests to preserve preassigned A and B in new col
            (
                4,
                "2022-12-01 00:00:00",
                "A",
                "B",
                2,
                "Yes",
                "Yes",
                "",
                "",
                "C",
                "D",
            ),  # Tests to preserve values due to business logic
            (
                5,
                "2022-12-01 00:00:00",
                "A",
                "",
                3,
                "No",
                "",
                "E",
                "",
                "B",
                "",
            ),  # swab barcode not correct and iqvia corrected
            (
                6,
                "2022-10-01 00:00:00",
                "A",
                None,
                3,
                "No",
                None,
                "F",
                None,
                "B",
                None,
            ),  # iqvia corrected
        ],
        schema="""
        id integer,
        participant_completion_window_start_datetime string,
        swab_sample_barcode string,
        blood_sample_barcode string,
        survey_response_dataset_major_version integer,
        swab_sample_barcode_correct string,
        blood_sample_barcode_correct string,
        swab_barcode_corrected string,
        blood_barcode_corrected string,
        swab_sample_barcode_user_entered string,
        blood_sample_barcode_user_entered string
        """,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2022-12-01 00:00:00", "A", "B", 3, "Yes", "Yes", None, None, "C", "D", "A", "B"),
            (2, "2022-12-01 00:00:00", "A", "B", 3, "No", "No", None, None, "C", "D", "C", "D"),
            (3, "2022-12-01 00:00:00", "A", "B", 3, "Yes", "Yes", None, None, None, None, "A", "B"),
            (4, "2022-12-01 00:00:00", "A", "B", 2, "Yes", "Yes", "", "", "C", "D", "A", "B"),
            (
                5,
                "2022-12-01 00:00:00",
                "A",
                "",
                3,
                "No",
                "",
                "E",
                "",
                "B",
                "",
                "E",
                "",
            ),  # swab barcode not correct and iqvia corrected
            (
                6,
                "2022-10-01 00:00:00",
                "A",
                None,
                3,
                "No",
                None,
                "F",
                None,
                "B",
                None,
                "F",
                None,
            ),
        ],
        schema="""
        id integer,
        participant_completion_window_start_datetime string,
        swab_sample_barcode string,
        blood_sample_barcode string,
        survey_response_dataset_major_version integer,
        swab_sample_barcode_correct string,
        blood_sample_barcode_correct string,
        swab_barcode_corrected string,
        blood_barcode_corrected string,
        swab_sample_barcode_user_entered string,
        blood_sample_barcode_user_entered string,
        swab_sample_barcode_combined string,
        blood_sample_barcode_combined string
        """,
    )

    output_df = replace_sample_barcode(input_df)
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_row_order=True)
