import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_school_year


@pytest.mark.parametrize(
    "expected_data",
    [("2021-09-02", "2013-01-19", 4), ("2021-09-02", "2013-08-12", 3)],
)
def test_assign_school_year(spark_session, expected_data):

    expected_schema = "visit_date string, dob string, school_year integer"

    expected_df = (
        spark_session.createDataFrame([expected_data], schema=expected_schema)
        # to_timestamp will not work for creation of test data, unix approach preferred
        .withColumn("visit_date", F.to_date(F.col("visit_date"))).withColumn("dob", F.to_date(F.col("dob")))
    )

    input_df = expected_df.drop("school_year")
    actual_df = assign_school_year(input_df, "school_year", "visit_date", "dob", None, None)

    assert_df_equality(actual_df, expected_df)
