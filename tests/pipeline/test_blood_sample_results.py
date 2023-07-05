import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_blood_results_df(test_blood_sample_results_output, regression_test_df):
    regression_test_df(
        test_blood_sample_results_output.drop("blood_results_source_file"),
        "antibody_test_well_id",
        "processed_blood_results",
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_blood_results_schema(regression_test_df_schema, test_blood_sample_results_output):
    regression_test_df_schema(test_blood_sample_results_output, "processed_blood_results")
