import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.lab_transformations import match_type_blood


def test_match_type(spark_session):
    schema = """
        blood_sample_barcode_combined string,
        blood_sample_barcode_survey_missing_lab boolean,
        blood_sample_barcode_lab_missing_survey boolean,
        file_date string,
        blood_sample_received_date string,
        survey_completed_datetime string,
        participant_completion_window_end_datetime string,
        blood_taken_datetime string,
        antibody_test_result_value integer
    """

    input_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            # swab logic save for later
            # ("A", False, True,  "2020-03-30", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 5), # no ppt match for lab result and result was <= 21 days ago
            # ("B", False, True,  "2020-03-30", "2020-03-10", "2020-03-26", "2020-01-04", "2020-03-06", 50), # no ppt match for lab result and result was > 21 days ago
            # ("C", True,  False, "2020-03-30", None,         "2020-03-26", "2020-01-04", "2020-03-06", None), # no lab result for ppt
            # ("D", True,  False, "2020-03-30", None,         "2020-03-10", "2020-03-20", "2020-03-01", None), # no lab result match for ppt and survey completed >= 7 days or window closed >= 7 days ago
            # ("E", False, False, "2020-03-30", "2020-03-08", "2020-03-10", "2020-03-20", "2020-03-01", 50), # lab result match where days between result_received and sample_taken <= 21
            # ("F", False, False, "2020-03-30", "2020-03-28", "2020-03-10", "2020-03-20", "2020-03-01", 50), # lab result match where days between result_received and sample_taken >= 21
            (None,False, True,  "2020-03-30", "2020-03-20", None,         None,         None,         5),  # no ppt match for lab results and lab result <= 28 days ago
            (None,False, True,  "2020-03-30", "2020-03-01", None,         None,         None,         5),  # no ppt match for lab result and lab result was > 28 days ago
            (None,True,  False, "2020-03-30", None,         "2020-03-26", "2020-03-30", "2020-03-01", None),  # no lab match for ppt and survey_completed <= 7 days ago or window closed <= 7 days ago
            (None,True,  False, "2020-03-30", None,         "2020-03-10", "2020-03-20", "2020-03-01", None),  # no lab match for ppt and survey_completed >= 7 days ago or window closed >= 7 days ago
            ("E", False, False, "2020-03-30", "2020-03-08", "2020-03-10", "2020-03-20", "2020-03-01", 5),  # ppt match where days between result received and sample_taken <= 28 and negative
            ("F", False, False, "2020-03-30", "2020-03-08", "2020-03-10", "2020-03-20", "2020-03-01", 50),  # ppt match where days between result received and sample_taken <= 28 and positive
            ("G", False, False, "2020-03-30", "2020-03-08", "2020-03-10", "2020-03-20", "2020-03-01", 250),  # ppt match where days between result received and sample_taken <= 28 and higher level
            ("H", False, False, "2020-03-30", "2020-04-01", "2020-03-10", "2020-03-20", "2020-03-01", 50),  # ppt match where days between result received and sample_taken >= 28
        ],
        # fmt: on
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (None,False, True,  "2020-03-30", "2020-03-20", None,         None,         None,         5,    None,               None,                                        None), # no ppt match for lab results and lab result <= 28 days ago
            (None,False, True,  "2020-03-30", "2020-03-01", None,         None,         None,         5,    "lab orphan",       "test void",                                 None), # no ppt match for lab result and lab result was > 28 days ago
            (None, True, False, "2020-03-30", None,         "2020-03-26", "2020-03-30", "2020-03-01", None, None,               None,                                        None), # no lab match
            (None, True, False, "2020-03-30", None,         "2020-03-10", "2020-03-20", "2020-03-01", None, "survey orphan",    "test failed",                               None), # no lab match for ppt and survey_completed >= 7 days ago or window closed >= 7 days ago
            ("E", False, False, "2020-03-30", "2020-03-08", "2020-03-10", "2020-03-20", "2020-03-01", 5,    "matched",          "Negative for antibodies",                   None), # ppt match where days between result received and sample_taken <= 28 and negative
            ("F", False, False, "2020-03-30", "2020-03-08", "2020-03-10", "2020-03-20", "2020-03-01", 50,   "matched",          "Positive for antibodies",                   None), # ppt match where days between result received and sample_taken <= 28 and positive
            ("G", False, False, "2020-03-30", "2020-03-08", "2020-03-10", "2020-03-20", "2020-03-01", 250,  "matched",          "Positive for antibodies at a higher level", None), # ppt match where days between result received and sample_taken <= 28 and higher level
            ("H", False, False, "2020-03-30", "2020-04-01", "2020-03-10", "2020-03-20", "2020-03-01", 50,   "matched",          "test void",                                 "mapped to string"), # ppt match where days between result received and sample_taken >= 28
        ],
        # fmt: on
        schema=schema
        + ", match_type_blood string, match_result_blood string, void_reason_blood string",
    )

    output_df = match_type_blood(input_df)
    assert_df_equality(output_df, expected_df)
