import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_survey_response_data_version_1_df(test_survey_response_data_version_1_data_description, regression_test_df):
    regression_test_df(
        test_survey_response_data_version_1_data_description.drop("survey_response_source_file"),
        "participant_completion_window_id",
        "processed_responses_phm",  # Check
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_survey_response_data_version_1_schema(
    regression_test_df_schema, test_survey_response_data_version_1_data_description
):
    regression_test_df_schema(test_survey_response_data_version_1_data_description, "processed_responses_phm")  # check
