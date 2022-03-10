import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_age_at_date


@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        data=[("2021-07-21", "1995-02-05", 26), ("2021-07-21", None, None)],
        schema="base_date string, date_of_birth string, age_at_date integer",
    )


def test_assign_age_at_date(spark_session, expected_df):
    expected_df = expected_df.withColumn("base_date", F.to_date(F.col("base_date"))).withColumn(
        "date_of_birth", F.to_date(F.col("date_of_birth"))
    )

    input_df = expected_df.drop(F.col("age_at_date"))

    actual_df = assign_age_at_date(input_df, "age_at_date", "base_date", "date_of_birth")

    assert_df_equality(actual_df, expected_df)
