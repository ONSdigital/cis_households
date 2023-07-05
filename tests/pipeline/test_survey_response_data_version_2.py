import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_survey_response_data_version_2_df(test_survey_response_data_version_2_data_description, regression_test_df):
    regression_test_df(
        test_survey_response_data_version_2_data_description.drop("survey_response_source_file"),
        "visit_id",
        "processed_responses_v2",
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_survey_response_data_version_2_schema(
    regression_test_df_schema, test_survey_response_data_version_2_data_description
):
    regression_test_df_schema(test_survey_response_data_version_2_data_description, "processed_responses_v2")
