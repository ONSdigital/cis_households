import json

import pytest
from pandas import NaT
from pandas import Timestamp
from pyspark.sql import SparkSession
from pytest_regressions.data_regression import RegressionYamlDumper


def timestamp_representer(dumper, timestamp):
    """Custom represented for using Timestamp type in data_regression testing"""
    return dumper.represent_data(timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"))


RegressionYamlDumper.add_custom_yaml_representer(Timestamp, timestamp_representer)


def nat_representer(dumper, nat):
    """Custom represented for using Timestamp type in data_regression testing"""
    return dumper.represent_data("NaT")


RegressionYamlDumper.add_custom_yaml_representer(NaT, timestamp_representer)


@pytest.fixture(scope="session")
def spark_session():
    """Session-wide SparkSession to optimise testing PySpark functions."""
    spark_session = SparkSession.builder.master("local[*]").getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def pandas_df_to_temporary_csv(tmp_path):
    """Provides a function to write a pandas dataframe to a temporary csv file with function scope."""

    def _pandas_df_to_temporary_csv(pandas_df, sep=","):
        temporary_csv_path = tmp_path / "temp.csv"
        pandas_df.to_csv(temporary_csv_path, sep=sep, header=True, index=False, na_rep="")
        return temporary_csv_path

    return _pandas_df_to_temporary_csv


@pytest.fixture(scope="function")
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
        data_regression.check(json.loads(df.schema.json()), f"{test_file_label}_schema_regression")
        data_regression.check(
            df.toPandas().sort_values(sort_by, axis="index").to_dict("records"), f"{test_file_label}_data_regression"
        )

    return _regression_test_df
