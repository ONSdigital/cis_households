import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.survey_responses_version_2_ETL import (
    extract_validate_transform_survey_responses_version_2_delta,
)
from dummy_data_generation.schemas import get_voyager_2_data_description


@pytest.fixture
def responses_v2_survey_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate dummy survey responses v2 delta.
    """
    schema = Schema(schema=get_voyager_2_data_description(mimesis_field, ["ONS00000000"], ["ONS00000000"]))
    pandas_df = pd.DataFrame(schema.create(iterations=10))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep="|")
    processed_df = extract_validate_transform_survey_responses_version_2_delta(csv_file_path.as_posix())
    return processed_df


@pytest.mark.integration
def test_responses_version_2_delta_df(responses_v2_survey_ETL_output, regression_test_df):
    regression_test_df(
        responses_v2_survey_ETL_output.drop("survey_responses_v2_source_file"), "visit_id", "processed_responses_v2"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.integration
def test_responses_version_2_delta_schema(regression_test_df_schema, responses_v2_survey_ETL_output):
    regression_test_df_schema(responses_v2_survey_ETL_output, "processed_responses_v2")
