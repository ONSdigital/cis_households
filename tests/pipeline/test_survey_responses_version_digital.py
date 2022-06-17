import pytest


@pytest.mark.integration
def test_responses_digital_df(responses_digital_ETL_output, regression_test_df):
    regression_test_df(
        responses_digital_ETL_output.drop("survey_response_source_file"),
        "participant_completion_window_id",
        "processed_responses_digital",  # Check
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.integration
def test_responses_digital_schema(regression_test_df_schema, responses_digital_ETL_output):
    regression_test_df_schema(responses_digital_ETL_output, "processed_responses_digital")  # check
