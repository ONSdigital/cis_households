import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark_session = SparkSession.builder.master("local[*]").getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def pandas_df_to_temporary_csv(tmp_path):
    def _(pandas_df):
        temporary_csv_path = tmp_path / "temp.csv"
        pandas_df.to_csv(temporary_csv_path, header=True, index=False)
        return temporary_csv_path

    return _
