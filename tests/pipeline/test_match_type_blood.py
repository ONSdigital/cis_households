import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.phm import match_type_blood


def test_match_type_blood(spark_session):
    schema = """
        blood_sample_barcode_combined string,
        file_date string,
        blood_sample_received_date string,
        survey_completed_datetime string,
        participant_completion_window_end_datetime string,
        blood_taken_datetime string,
        antibody_test_result_value integer
    """

    input_df = spark_session.createDataFrame(
        data=[
            ("A", "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 5),
            ("B", "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 50),
            ("C", "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 250),
            (None, "2020-01-04", "2020-03-10", "2020-01-03", "2019-12-01", "2020-03-06", 50),
            ("D", "2020-10-04", "2020-12-10", "2020-03-26", "2020-01-04", "2020-03-06", 50),
            ("E", "2020-10-04", "2020-03-10", "2020-05-26", "2020-01-04", "2020-03-06", 50),
            (None, "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 50),
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (
                "A",
                "2020-01-04",
                "2020-03-10",
                "2020-03-26",
                "2020-01-04",
                "2020-03-06",
                5,
                "matched",
                "Negative for antibodies",
                None,
            ),
            (
                "B",
                "2020-01-04",
                "2020-03-10",
                "2020-03-26",
                "2020-01-04",
                "2020-03-06",
                50,
                "matched",
                "Positive for antibodies",
                None,
            ),
            (
                "C",
                "2020-01-04",
                "2020-03-10",
                "2020-03-26",
                "2020-01-04",
                "2020-03-06",
                250,
                "matched",
                "Positive for antibodies at a higher level",
                None,
            ),
            (
                None,
                "2020-01-04",
                "2020-03-10",
                "2020-01-03",
                "2019-12-01",
                "2020-03-06",
                50,
                "survey orphan",
                "test failed",
                None,
            ),
            (
                "D",
                "2020-10-04",
                "2020-12-10",
                "2020-03-26",
                "2020-01-04",
                "2020-03-06",
                50,
                "matched",
                "test void",
                "mapped to string",
            ),
            (
                "E",
                "2020-10-04",
                "2020-03-10",
                "2020-05-26",
                "2020-01-04",
                "2020-03-06",
                50,
                "lab orphan",
                "test void",
                None,
            ),
            (None, "2020-01-04", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 50, None, None, None),
        ],
        schema=schema + ", match_type_blood string, match_result_blood string, void_reason_blood string",
    )

    output_df = match_type_blood(input_df)

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
