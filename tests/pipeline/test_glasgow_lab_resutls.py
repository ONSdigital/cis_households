import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_glasgow_lab_results_df(glasgow_lab_results_output, regression_test_df):
    regression_test_df(
        glasgow_lab_results_output.drop("lab_results_source_file"), "pcr_lab_id", "processed_glasgow_lab_results"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_glasgow_lab_results_schema(regression_test_df_schema, glasgow_lab_results_output):
    regression_test_df_schema(glasgow_lab_results_output, "processed_glasgow_lab_results")
