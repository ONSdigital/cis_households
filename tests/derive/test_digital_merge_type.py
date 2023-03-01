import pytest
from chispa import assert_df_equality

from cishouseholds.derive import derive_digital_merge_type


def test_digital_merge_type(spark_session):
    schema = """
        survey_completion_status string,
        form_start_datetime string,
        participant_completion_window_end_datetime string,
        file_date string,
        id integer
    """

    input_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            ("Submitted"          ,  None        , None        , "2020-04-27", 1),
            ("Completed"          ,  None        , None        , "2020-04-27", 2),
            ("New"                ,  "2020-03-30", "2020-04-27", "2020-04-26", 3),
            ("Partially Completed",  "2020-03-30", "2020-04-27", "2020-04-26", 4),
            ("New"                ,  None        , "2020-04-27", "2020-04-26", 5),
            ("Partially Completed",  "2020-03-30", "2020-04-26", "2020-04-26", 6),
            ("Partially Completed",  "2020-03-30", "2020-04-26", "2020-04-27", 7),
            ("New"                ,  "2020-03-30", "2020-04-26", "2020-04-27", 8), ## end_datetime equals file_date
            ("New"                ,  "2020-03-30", "2020-04-26", "2020-04-27", 9), ## end_datetime < file_date
            ("New"                ,  None, "2020-04-26", "2020-04-26",         10), ## end_datetime equals file_date
            ("New"                ,  None, "2020-04-26", "2020-04-27",         11), ## end_datetime < file_date
        ],
        # fmt: on
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            ("Submitted"          ,  None        , None        , "2020-04-27", 1,          "Matched"),
            ("Completed"          ,  None        , None        , "2020-04-27", 2,          "Matched"),
            ("New"                ,  "2020-03-30", "2020-04-27", "2020-04-26", 3, "Temporary Orphan"),
            ("Partially Completed",  "2020-03-30", "2020-04-27", "2020-04-26", 4, "Temporary Orphan"),
            ("New"                ,  None        , "2020-04-27", "2020-04-26", 5, "Potential Orphan"),
            ("Partially Completed",  "2020-03-30", "2020-04-26", "2020-04-26", 6,          "Matched"),
            ("Partially Completed",  "2020-03-30", "2020-04-26", "2020-04-27", 7,          "Matched"),
            ("New"                ,  "2020-03-30", "2020-04-26", "2020-04-27", 8,          "Matched"),
            ("New"                ,  "2020-03-30", "2020-04-26", "2020-04-27", 9,          "Matched"),
            ("New"                ,  None, "2020-04-26", "2020-04-26",         10,"Permanent Orphan"),
            ("New"                ,  None, "2020-04-26", "2020-04-27",         11,"Permanent Orphan"),
        ],
        # fmt: on
        schema=schema
        + ",digital_merge_type string",
    )

    output_df = derive_digital_merge_type(input_df, column_name_to_assign="digital_merge_type")
    assert_df_equality(output_df, expected_df)
