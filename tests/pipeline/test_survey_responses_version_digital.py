import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.input_file_stages import cis_digital_parameters
from cishouseholds.pipeline.input_file_stages import generate_input_processing_function
from dummy_data_generation.schemas import get_survey_responses_digital_data_description


@pytest.fixture
def responses_digital_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate dummy survey responses digital.
    """
    schema = Schema(
        schema=get_survey_responses_digital_data_description(mimesis_field, ["ONS00000000"], ["ONS00000000"])
    )
    pandas_df = pd.DataFrame(schema.create(iterations=10))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep="|")
    processing_function = generate_input_processing_function(**cis_digital_parameters, include_hadoop_read_write=False)
    processed_df = processing_function(resource_path=csv_file_path)
    return processed_df


@pytest.mark.integration
def test_responses_digital_df(responses_digital_ETL_output, regression_test_df):
    regression_test_df(
        responses_digital_ETL_output.drop("survey_response_source_file"),
        "participant_completion_window_id",
        "processed_responses_digital",  # Check
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.integration
def test_responses_digital_schema(regression_test_df_schema, responses_digital_ETL_output):
    regression_test_df_schema(responses_digital_ETL_output, "processed_responses_digital")  # check
