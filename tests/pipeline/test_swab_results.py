import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_swab_results_df(swab_results_output, regression_test_df):
    regression_test_df(
        swab_results_output.drop("swab_results_source_file"), "pcr_result_recorded_datetime", "processed_swab_results"
    )


@pytest.mark.regression
@pytest.mark.integration
def test_swab_results_schema(regression_test_df_schema, swab_results_output):
    regression_test_df_schema(swab_results_output, "processed_swab_results")
