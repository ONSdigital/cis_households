import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_participant_extract_digital_df(participants_digital_ETL_output, regression_test_df):
    regression_test_df(
        participants_digital_ETL_output.drop("survey_response_source_file"),
        "participant_id",
        "processed_participants_digital",  # Check
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_participant_extract_digital_schema(regression_test_df_schema, participants_digital_ETL_output):
    regression_test_df_schema(participants_digital_ETL_output, "processed_participants_digital")  # check
