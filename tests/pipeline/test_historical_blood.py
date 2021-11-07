import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.blood_delta_ETL import transform_blood_delta
from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import historical_blood_variable_name_map
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.validation_schema import historical_blood_validation_schema
from dummy_data_generation.schemas import get_historical_blood_data_description


@pytest.fixture
def historical_blood_delta_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate historical bloods file.
    """
    schema = Schema(schema=get_historical_blood_data_description(mimesis_field))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, filename="N.csv")
    processed_df = extract_validate_transform_input_data(
        csv_file_path.as_posix(),
        historical_blood_variable_name_map,
        blood_datetime_map,
        historical_blood_validation_schema,
        transform_blood_delta,
    )

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
