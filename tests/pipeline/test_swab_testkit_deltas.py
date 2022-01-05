import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.input_file_processing import generate_input_processing_function
from cishouseholds.pipeline.input_file_processing import swab_delta_parameters_testKit
from dummy_data_generation.schemas import get_swab_testkit_data_description


@pytest.fixture
def swab_testkit_delta_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate lab swab_testkit file as pandas df.
    """
    schema = Schema(schema=get_swab_testkit_data_description(mimesis_field))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df)
    processing_function = generate_input_processing_function(
        **swab_delta_parameters_testKit, include_hadoop_read_write=False
    )
    processed_df = processing_function(resource_path=csv_file_path.as_posix())

    return processed_df


@pytest.mark.integration
def test_swab_testkit_delta_ETL_df(swab_testkit_delta_ETL_output, regression_test_df):
    regression_test_df(
        swab_testkit_delta_ETL_output.drop("swab_test_source_file"), "swab_sample_barcode", "processed_swab"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.integration
def test_swab_testkit_delta_ETL_schema(swab_testkit_delta_ETL_output, regression_test_df_schema):
    regression_test_df_schema(swab_testkit_delta_ETL_output, "processed_swab")
