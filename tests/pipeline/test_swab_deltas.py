import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.swab_delta_ETL import swab_delta_ETL
from dummy_data_generation.schemas import get_swab_data_description


@pytest.fixture
def swab_dummy_df(mimesis_field):
    """
    Generate lab swab file as pandas df.
    """
    schema = Schema(schema=get_swab_data_description(mimesis_field))
    pandas_df = pd.DataFrame(schema.create(iterations=5))

    return pandas_df


def test_swab_delta_ETL_end_to_end(regression_test_df, swab_dummy_df, pandas_df_to_temporary_csv):
    """
    Test that valid example data flows through the ETL from a csv file.
    """
    csv_file = pandas_df_to_temporary_csv(swab_dummy_df)
    processed_df = swab_delta_ETL(csv_file.as_posix())
    regression_test_df(processed_df, "swab_sample_barcode", "processed_swab")
