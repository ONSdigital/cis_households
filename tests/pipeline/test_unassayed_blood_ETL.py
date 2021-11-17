import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import unassayed_bloods_variable_name_map
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.unassayed_blood_ETL import transform_unassayed_blood
from cishouseholds.pipeline.validation_schema import unassayed_blood_validation_schema
from dummy_data_generation.schemas import get_unassayed_blood_data_description


@pytest.fixture
def unassayed_blood_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate unassayed bloods file.
    """
    schema = Schema(schema=get_unassayed_blood_data_description(mimesis_field))
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df)
    processed_df = extract_validate_transform_input_data(
        csv_file_path.as_posix(),
        unassayed_bloods_variable_name_map,
        blood_datetime_map,
        unassayed_blood_validation_schema,
        transform_unassayed_blood,
    )

    return processed_df


@pytest.mark.integration
def test_unassayed_blood_ETL_df(regression_test_df, unassayed_blood_ETL_output):
    regression_test_df(
        unassayed_blood_ETL_output.drop("unassayed_blood_source_file"), "blood_sample_barcode", "unassayed_blood"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.integration
def test_unassayed_blood_ETL_schema(regression_test_df_schema, unassayed_blood_ETL_output):
    regression_test_df_schema(unassayed_blood_ETL_output, "unassayed_blood")
