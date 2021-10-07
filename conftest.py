import json

import pytest
from pyspark.sql import SparkSession


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
        pandas_df.to_csv(temporary_csv_path, sep=sep, header=True, index=False)
        return temporary_csv_path

    return _pandas_df_to_temporary_csv


@pytest.fixture(scope="function")
def regression_test_df(data_regression):
    """Regression test both the schema and data from a PySpark dataframe"""

    def _regression_test_df(df, sort_by, test_file_label):
        data_regression.check(json.loads(df.schema.json()), f"{test_file_label}_schema_regression")
        data_regression.check(
            df.toPandas().sort_values(sort_by, axis="index").to_dict("records"), f"{test_file_label}_data_regression"
        )

    return _regression_test_df
