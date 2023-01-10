import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_swab_results_df(swab_results_output, regression_test_df):
    regression_test_df(
        swab_results_output.drop("swab_results_source_file"), "pcr_lab_id", "processed_swab_results"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_swab_results_schema(regression_test_df_schema, swab_results_output):
    regression_test_df_schema(swab_results_output, "processed_swab_results")
