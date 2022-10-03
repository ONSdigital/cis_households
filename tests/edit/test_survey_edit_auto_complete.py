from datetime import datetime

from chispa import assert_df_equality

from cishouseholds.edit import survey_edit_auto_complete


def test_survey_edit_auto_complete(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("In progress", "12-09-2022", "12-09-2022", None),
            ("In progress", "12-09-2021", "14-09-2022", "yes"),
            ("In progress", "12-09-2021", "14-09-2022", None),
            ("In progress", "10-01-2022", "01-01-2022", None),
            ("Submitted", "12-09-2022", "12-09-2022", "yes"),
            ("Submitted", "12-09-2021", "14-09-2022", None),
            ("Submitted", "10-01-2022", "01-01-2022", "yes"),
        ],
        schema="column_name_to_assign string, completion_window string, file_date string, last_question string",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("In progress", "12-09-2022", "12-09-2022", None),
            ("Auto Completed", "12-09-2021", "14-09-2022", "yes"),
            ("In progress", "12-09-2021", "14-09-2022", None),
            ("In progress", "10-01-2022", "01-01-2022", None),
            ("Submitted", "12-09-2022", "12-09-2022", "yes"),
            ("Submitted", "12-09-2021", "14-09-2022", None),
            ("Submitted", "10-01-2022", "01-01-2022", "yes"),
        ],
        schema="column_name_to_assign string, completion_window string, file_date string, last_question string",
    )

    output_df = survey_edit_auto_complete(
        input_df, "column_name_to_assign", "completion_window", "last_question", "2022-09-20"
    )
    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True)
