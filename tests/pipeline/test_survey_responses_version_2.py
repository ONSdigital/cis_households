import pandas as pd
import pytest
from mimesis.schema import Field
from mimesis.schema import Schema

from cishouseholds.edit import rename_column_names
from cishouseholds.pipeline.input_variable_names import iqvia_v2_variable_name_map
from cishouseholds.pipeline.survey_responses_version_2_ETL import survey_responses_version_2_ETL
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_version_2_delta
from dummy_data_generation.schemas import voyager_2_data_description

_ = Field("en-gb", seed=69)


@pytest.fixture
def iqvia_v2_survey_dummy_df(spark_session):
    """
    Generate dummy IQVIA v2 survey file.
    """
    schema = Schema(schema=voyager_2_data_description)
    # iterations increased to 50 to prevent issue of all null values occuring inline
    pandas_df = pd.DataFrame(schema.create(iterations=50))
    iqvia_v2_survey_dummy_df = spark_session.createDataFrame(pandas_df)
    iqvia_v2_survey_dummy_df = rename_column_names(iqvia_v2_survey_dummy_df, iqvia_v2_variable_name_map)
    return iqvia_v2_survey_dummy_df


def test_transform_survey_responses_version_2_delta(iqvia_v2_survey_dummy_df, spark_session, data_regression):
    transformed_df = (
        transform_survey_responses_version_2_delta(spark_session, iqvia_v2_survey_dummy_df).toPandas().to_dict()
    )
    data_regression.check(transformed_df)


def test_iqvia_version_2_ETL_delta_ETL_end_to_end(iqvia_v2_survey_dummy_df, pandas_df_to_temporary_csv):
    """
    Test that valid example data flows through the ETL from a csv file.
    """
    iqvia_v2_survey_dummy_df = iqvia_v2_survey_dummy_df.toPandas()
    csv_file = pandas_df_to_temporary_csv(iqvia_v2_survey_dummy_df)
    survey_responses_version_2_ETL(csv_file.as_posix())
