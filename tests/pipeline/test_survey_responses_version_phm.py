import pyspark.sql.functions as F
import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_responses_phm_df(responses_phm_ETL_output, regression_test_df):
    regression_test_df(
        responses_phm_ETL_output.drop("survey_response_source_file"),
        "participant_completion_window_id",
        "processed_responses_digital",  # Check
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_responses_phm_schema(regression_test_df_schema, responses_phm_ETL_output):
    regression_test_df_schema(responses_phm_ETL_output, "processed_responses_phm")  # check
