import pandas as pd
import pytest
from mimesis.schema import Field
from mimesis.schema import Schema

from cishouseholds.edit import rename_column_names
from cishouseholds.pipeline.bloods_delta_ETL import bloods_delta_ETL
from cishouseholds.pipeline.bloods_delta_ETL import transform_bloods_delta
from cishouseholds.pipeline.input_variable_names import bloods_variable_name_map
from dummy_data_generation.schemas import blood_data_description

_ = Field("en-gb", seed=42)


@pytest.fixture
def bloods_dummy_df():
    """
    Generate lab bloods file.
    """
    schema = Schema(schema=blood_data_description)
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    return pandas_df


def test_transform_bloods_delta(bloods_dummy_df, spark_session, data_regression):

    bloods_dummy_df = spark_session.createDataFrame(bloods_dummy_df)
    bloods_dummy_df = rename_column_names(bloods_dummy_df, bloods_variable_name_map)
    transformed_df = transform_bloods_delta(bloods_dummy_df).toPandas().to_dict()
    data_regression.check(transformed_df)


def test_bloods_delta_ETL_end_to_end(bloods_dummy_df, pandas_df_to_temporary_csv):
    """
    Test that valid example data flows through the ETL from a csv file.
    """
    csv_file = pandas_df_to_temporary_csv(bloods_dummy_df)
    bloods_delta_ETL(csv_file.as_posix())
