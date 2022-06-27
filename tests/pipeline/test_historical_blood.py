import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.input_file_stages import generate_input_processing_function
from cishouseholds.pipeline.input_file_stages import historical_blood_parameters
from dummy_data_generation.schemas import get_historical_blood_data_description


@pytest.fixture
def historical_blood_delta_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate historical bloods file.
    """
    schema = Schema(schema=get_historical_blood_data_description(mimesis_field))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, filename="N.csv")
    processing_function = generate_input_processing_function(
        **historical_blood_parameters, include_hadoop_read_write=False
    )
    processed_df = processing_function(resource_path=csv_file_path)
    return processed_df


@pytest.mark.integration
def test_historical_blood_delta_ETL_df(regression_test_df, historical_blood_delta_ETL_output):
    regression_test_df(
        historical_blood_delta_ETL_output.drop("blood_test_source_file"),
        "blood_sample_barcode",
        "processed_historical_blood",
    )  # removes filename column to account for variation in filename caused by regression


@pytest.mark.integration
def test_historical_blood_delta_ETL_schema(regression_test_df_schema, historical_blood_delta_ETL_output):
    regression_test_df_schema(historical_blood_delta_ETL_output, "processed_historical_blood")
