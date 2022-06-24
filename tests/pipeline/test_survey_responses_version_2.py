import pytest


@pytest.mark.integration
def test_responses_version_2_delta_df(responses_v2_survey_ETL_output, regression_test_df):
    regression_test_df(
        responses_v2_survey_ETL_output.drop("survey_response_source_file"), "visit_id", "processed_responses_v2"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.integration
def test_responses_version_2_delta_schema(regression_test_df_schema, responses_v2_survey_ETL_output):
    regression_test_df_schema(responses_v2_survey_ETL_output, "processed_responses_v2")
