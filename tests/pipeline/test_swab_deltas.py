import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.swab_delta_ETL import extract_validate_transform_swab_delta
from dummy_data_generation.schemas import get_swab_data_description


@pytest.fixture
def swab_delta_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate lab swab file as pandas df.
    """
    schema = Schema(schema=get_swab_data_description(mimesis_field))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df)
    processed_df = extract_validate_transform_swab_delta(csv_file_path.as_posix())

    return processed_df


@pytest.mark.integration
def test_swab_delta_ETL_df(swab_delta_ETL_output, regression_test_df):
    regression_test_df(swab_delta_ETL_output, "swab_sample_barcode", "processed_swab")


@pytest.mark.integration
def test_swab_delta_ETL_schema(swab_delta_ETL_output, regression_test_df_schema):
    regression_test_df_schema(swab_delta_ETL_output, "processed_swab")
