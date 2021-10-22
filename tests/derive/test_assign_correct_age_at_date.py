import pyspark.sql.functions as F

import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_correct_age_at_date


@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        data=[
            ("2021-07-21", "1995-07-21", 26),
            ("2021-07-21", "1999-07-20", 22),
            ("2021-07-21", "1946-03-17", 75),
            ("2021-07-21", "1970-10-03", 50),
            ("2021-07-21", None, None),
        ],
        schema="reference_date string, date_of_birth string, age_at_date integer",
    )


def test_assign_correct_age_at_date(spark_session, expected_df):
    expected_df = expected_df.withColumn("reference_date", F.to_date(F.col("reference_date"))).withColumn(
        "date_of_birth", F.to_date(F.col("date_of_birth"))
    )

    input_df = expected_df.drop(F.col("age_at_date"))

    actual_df = assign_correct_age_at_date(input_df, "age_at_date", "reference_date", "date_of_birth")

    assert_df_equality(actual_df, expected_df)
