import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.phm import match_type_blood


def test_match_type(spark_session):
    schema = """
        blood_sample_barcode_combined string,
        blood_sample_barcode_missing_lab boolean,
        blood_sample_barcode_missing_survey boolean,
        file_date string,
        blood_sample_received_date string,
        survey_completed_datetime string,
        participant_completion_window_end_datetime string,
        blood_taken_datetime string,
        antibody_test_result_value integer
    """

    input_df = spark_session.createDataFrame(
        data=[
            ("A", False, False, "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 5),
            ("B", False, False, "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 50),
            ("C", False, False, "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 250),
            (None, False, True, "2020-01-04", "2020-03-10", "2020-01-03", "2019-12-01", "2020-03-06", 50),
            ("D", False, False, "2020-10-04", "2020-12-10", "2020-03-26", "2020-01-04", "2020-03-06", 50),
            (None, True, False, "2020-10-04", "2020-03-10", "2020-05-26", "2020-01-04", "2020-03-06", 50),
            (None, True, False, "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 50),
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            ("A", False, False, "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 5,   "matched",      "Negative for antibodies",                   None),
            ("B", False, False, "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 50,  "matched",      "Positive for antibodies",                   None),
            ("C", False, False, "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 250, "matched",      "Positive for antibodies at a higher level", None),
            (None,False, True,  "2020-01-04", "2020-03-10", "2020-01-03", "2019-12-01", "2020-03-06", 50,  "survey orphan","test failed",                               None),
            ("D", False, False, "2020-10-04", "2020-12-10", "2020-03-26", "2020-01-04", "2020-03-06", 50,  "matched",      "test void",                                "mapped to string"),
            (None,True,  False, "2020-10-04", "2020-03-10", "2020-05-26", "2020-01-04", "2020-03-06", 50,  "lab orphan",   "test void",                                 None),
            (None,True,  False, "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 50,   None,           None,                                       None),
        ],
        # fmt: on
        schema=schema
        + ", match_type_blood string, match_result_blood string, void_reason_blood string",
    )

    output_df = match_type_blood(input_df)

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
