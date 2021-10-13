import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.blood_delta_ETL import extract_validate_transform_blood_delta
from dummy_data_generation.schemas import get_blood_data_description


@pytest.fixture
def blood_dummy_df(mimesis_field):
    """
    Generate lab bloods file.
    """
    schema = Schema(schema=get_blood_data_description(mimesis_field, "N"))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    return pandas_df


def test_blood_delta_ETL_end_to_end(regression_test_df, blood_dummy_df, pandas_df_to_temporary_csv):
    """
    Test that valid example data flows through the ETL from a csv file.
    """
    csv_file = pandas_df_to_temporary_csv(blood_dummy_df)
    processed_df = extract_validate_transform_blood_delta(csv_file.as_posix())
    regression_test_df(processed_df, "blood_sample_barcode", "processed_blood")
