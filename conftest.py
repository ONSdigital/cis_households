import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from mimesis.schema import Field
from mimesis.schema import Schema
from pandas import Timestamp
from pyspark.sql import SparkSession
from pytest_regressions.data_regression import RegressionYamlDumper

from cishouseholds.pipeline.input_file_stages import cis_digital_parameters
from cishouseholds.pipeline.input_file_stages import generate_input_processing_function
from cishouseholds.pipeline.input_file_stages import survey_responses_v0_parameters
from cishouseholds.pipeline.input_file_stages import survey_responses_v1_parameters
from cishouseholds.pipeline.input_file_stages import survey_responses_v2_parameters
from dummy_data_generation.helpers import CustomRandom
from dummy_data_generation.helpers_weight import Distribution
from dummy_data_generation.schemas import get_survey_responses_digital_data_description
from dummy_data_generation.schemas import get_voyager_0_data_description
from dummy_data_generation.schemas import get_voyager_1_data_description
from dummy_data_generation.schemas import get_voyager_2_data_description


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: mark integration tests (likely to run slowly)")


def timestamp_representer(dumper, timestamp):
    """Custom represented for using Timestamp type in data_regression testing"""
    return dumper.represent_data(timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"))


RegressionYamlDumper.add_custom_yaml_representer(Timestamp, timestamp_representer)


@pytest.fixture(scope="session")
def spark_session():
    """Session-wide SparkSession to optimise testing PySpark functions."""
    spark_session = (
        SparkSession.builder.master("local[*]")
        .config("spark.executor.memory", "6g")
        .config("spark.executor.cores", 3)
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", 3)
        .config("spark.sql.shuffle.partitions", 18)
        .config("spark.sql.crossJoin.enabled", "true")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="session")
def responses_v0_survey_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate dummy survey responses v0 delta.
    """
    schema = Schema(schema=get_voyager_0_data_description(mimesis_field, ["ONS00000000"], ["ONS00000000"]))
    pandas_df = pd.DataFrame(schema.create(iterations=10))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep="|")
    processing_function = generate_input_processing_function(
        **survey_responses_v0_parameters, include_hadoop_read_write=False
    )
    processed_df = processing_function(resource_path=csv_file_path.as_posix())
    return processed_df


@pytest.fixture(scope="session")
def responses_v1_survey_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate dummy survey responses v1 delta.
    """
    schema = Schema(schema=get_voyager_1_data_description(mimesis_field, ["ONS00000000"], ["ONS00000000"]))
    pandas_df = pd.DataFrame(schema.create(iterations=10))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep="|")
    processing_function = generate_input_processing_function(
        **survey_responses_v1_parameters, include_hadoop_read_write=False
    )
    processed_df = processing_function(resource_path=csv_file_path.as_posix())
    return processed_df


@pytest.fixture(scope="session")
def responses_v2_survey_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate dummy survey responses v2 delta.
    """
    schema = Schema(schema=get_voyager_2_data_description(mimesis_field, ["ONS00000000"], ["ONS00000000"]))
    pandas_df = pd.DataFrame(schema.create(iterations=10))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep="|")
    processing_function = generate_input_processing_function(
        **survey_responses_v2_parameters, include_hadoop_read_write=False
    )
    processed_df = processing_function(resource_path=csv_file_path.as_posix())
    return processed_df


@pytest.fixture(scope="session")
def responses_digital_ETL_output(mimesis_field, pandas_df_to_temporary_csv):
    """
    Generate dummy survey responses digital.
    """
    schema = Schema(
        schema=get_survey_responses_digital_data_description(mimesis_field, ["ONS00000000"], ["ONS00000000"])
    )
    pandas_df = pd.DataFrame(schema.create(iterations=10))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep="|")
    processing_function = generate_input_processing_function(**cis_digital_parameters, include_hadoop_read_write=False)
    processed_df = processing_function(resource_path=csv_file_path.as_posix())
    return processed_df


@pytest.fixture(scope="session")
def pandas_df_to_temporary_csv(tmpdir_factory):
    """Provides a function to write a pandas dataframe to a temporary csv file with function scope."""

    def _pandas_df_to_temporary_csv(pandas_df, sep=",", filename="temp.csv"):
        temporary_csv_path = Path(tmpdir_factory.mktemp("data") / filename)
        pandas_df.to_csv(temporary_csv_path, sep=sep, header=True, index=False, na_rep="")
        return temporary_csv_path

    return _pandas_df_to_temporary_csv


@pytest.fixture
def regression_test_df(data_regression):
    """
    Regression test both the schema and data from a PySpark dataframe

    Parameters
    ----------
    sort_by
        unique ID column to sort by. Must not contain nulls
    test_file_label
        text used to label regression test outputs
    """

    def _regression_test_df(df, sort_by, test_file_label):
        assert df.filter(df[sort_by].isNull()).count() == 0
        pandas_df = df.toPandas().replace({np.nan: None})  # Convert nan and nat to null
        data_regression.check(
            pandas_df.sort_values(sort_by, axis="index").to_dict("records"), f"{test_file_label}_data_regression"
        )

    return _regression_test_df


@pytest.fixture
def regression_test_df_schema(data_regression):
    """
    Regression test both the schema and data from a PySpark dataframe

    Parameters
    ----------
    sort_by
        unique ID column to sort by. Must not contain nulls
    test_file_label
        text used to label regression test outputs
    """

    def _regression_test_df(df, test_file_label):
        data_regression.check(json.loads(df.schema.json()), f"{test_file_label}_schema_regression")

    return _regression_test_df


@pytest.fixture(scope="session")
def mimesis_field():
    """Generate a new field for mimesis data generation, to ensure test data are independently generated"""
    return Field("en-gb", seed=42, providers=[Distribution, CustomRandom])
