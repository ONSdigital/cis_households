import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.unprocessed_blood_ETL import extract_validate_transform_unprocessed_blood
from dummy_data_generation.schemas import get_unprocessed_blood_description


@pytest.fixture
def unprocessed_blood_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate lab bloods file.
    """
    schema = Schema(schema=get_unprocessed_blood_description(mimesis_field, "N"))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df)
    processed_df = extract_validate_transform_unprocessed_blood(csv_file_path.as_posix())

    return processed_df


@pytest.mark.integration
def test_unprocessed_blood_ETL_df(regression_test_df, unprocessed_blood_ETL_output):
    regression_test_df(
        unprocessed_blood_ETL_output.drop("unprocessed_blood_source_file"), "sample_id", "unprocessed_blood"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.integration
def test_unprocessed_blood_ETL_schema(regression_test_df_schema, unprocessed_blood_ETL_output):
    regression_test_df_schema(unprocessed_blood_ETL_output, "unprocessed_blood")
