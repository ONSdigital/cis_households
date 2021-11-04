import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.swab_delta_ETL import transform_swab_delta
from cishouseholds.pipeline.timestamp_map import swab_datetime_map
from cishouseholds.pipeline.validation_schema import swab_validation_schema
from dummy_data_generation.schemas import get_swab_data_description


@pytest.fixture
def swab_delta_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate lab swab file as pandas df.
    """
    schema = Schema(schema=get_swab_data_description(mimesis_field))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df)
    processed_df = extract_validate_transform_input_data(
        csv_file_path.as_posix(),
        swab_variable_name_map,
        swab_datetime_map,
        swab_validation_schema,
        transform_swab_delta,
    )

    return processed_df


@pytest.mark.integration
def test_swab_delta_ETL_df(swab_delta_ETL_output, regression_test_df):
    regression_test_df(
        swab_delta_ETL_output.drop("swab_test_source_file"), "swab_sample_barcode", "processed_swab"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.integration
def test_swab_delta_ETL_schema(swab_delta_ETL_output, regression_test_df_schema):
    regression_test_df_schema(swab_delta_ETL_output, "processed_swab")
