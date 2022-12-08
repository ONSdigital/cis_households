import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_blood_results_df(blood_responses_output, regression_test_df):
    regression_test_df(
        blood_responses_output.drop("blood_results_source_file"), "blood_sample_barcode", "processed_blood_results"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_blood_results_schema(regression_test_df_schema, blood_responses_output):
    regression_test_df_schema(blood_responses_output, "processed_blood_results")
