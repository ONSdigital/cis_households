import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_participant_extract_digital_df(test_participant_data_ETL_output, regression_test_df):
    regression_test_df(
        test_participant_data_ETL_output.drop("participant_data_source_file"),
        "participant_id",
        "processed_participants_digital",  # Check
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_participant_extract_digital_schema(regression_test_df_schema, test_participant_data_ETL_output):
    regression_test_df_schema(test_participant_data_ETL_output, "processed_participants_digital")  # check
