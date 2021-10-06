import pandas as pd
import pytest
from mimesis.schema import Schema

from cishouseholds.edit import rename_column_names
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.swab_delta_ETL import swab_delta_ETL
from cishouseholds.pipeline.swab_delta_ETL import transform_swab_delta
from dummy_data_generation.schemas import swab_data_description


@pytest.fixture
def swab_dummy_df():
    """
    Generate lab swab file.
    """
    schema = Schema(schema=swab_data_description)
    pandas_df = pd.DataFrame(schema.create(iterations=5))

    return pandas_df


def test_transform_swab_delta_ETL(swab_dummy_df, spark_session, data_regression):
    """
    Test that swab transformation provides reproducible results.
    """
    swab_dummy_df = spark_session.createDataFrame(swab_dummy_df)
    swab_dummy_df = rename_column_names(swab_dummy_df, swab_variable_name_map)
    transformed_df = transform_swab_delta(spark_session, swab_dummy_df).toPandas().to_dict()
    data_regression.check(transformed_df)


def test_swab_delta_ETL_end_to_end(swab_dummy_df, pandas_df_to_temporary_csv):
    """
    Test that valid example data flows through the ETL from a csv file.
    """
    csv_file = pandas_df_to_temporary_csv(swab_dummy_df)
    swab_delta_ETL(csv_file.as_posix())
