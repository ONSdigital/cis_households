import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.survey_responses_version_2_ETL import (
    extract_validate_transform_survey_responses_version_2_delta,
)
from dummy_data_generation.schemas import get_voyager_2_data_description


@pytest.fixture
def responses_v2_survey_dummy_df(mimesis_field):
    """
    Generate dummy survey responses v2 delta.
    """
    schema = Schema(schema=get_voyager_2_data_description(mimesis_field, ["ONS00000000"], ["ONS00000000"]))
    pandas_df = pd.DataFrame(schema.create(iterations=10))
    return pandas_df


@pytest.mark.integration
def test_responses_version_2_delta_ETL_without_load(
    regression_test_df, responses_v2_survey_dummy_df, pandas_df_to_temporary_csv
):
    """
    Test that valid example data flows through the ETL from a csv file.
    """
    csv_file = pandas_df_to_temporary_csv(responses_v2_survey_dummy_df, sep="|")
    processed_df = extract_validate_transform_survey_responses_version_2_delta(csv_file.as_posix())
    regression_test_df(processed_df, "visit_id", "processed_responses_v2")
