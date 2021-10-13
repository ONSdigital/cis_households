import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.blood_delta_ETL import extract_validate_transform_blood_delta
from dummy_data_generation.schemas import get_blood_data_description


@pytest.fixture
def blood_delta_ETL_output(mimesis_field):
    """
    Generate lab bloods file.
    """
    schema = Schema(schema=get_blood_data_description(mimesis_field, "N"))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    csv_file = pandas_df(pandas_df)
    processed_df = extract_validate_transform_blood_delta(csv_file.as_posix())

    return processed_df


@pytest.mark.integration
def test_blood_delta_ETL_df(regression_test_df, blood_delta_ETL_output):
    regression_test_df(blood_delta_ETL_output, "blood_sample_barcode", "processed_blood")


@pytest.mark.integration
def test_blood_delta_ETL_schema(regression_test_schema, blood_delta_ETL_output):
    regression_test_schema(blood_delta_ETL_output, "processed_blood")
