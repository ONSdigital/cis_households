import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_column_to_date_string


@pytest.mark.parametrize(
    "expected_data",
    [("1966-07-30 15:00:00", "1966-07-30"), (None, None)],
)
def test_convert_to_date(spark_session, expected_data):

    expected_schema = "time_example string, date_example string"

    expected_df = (
        spark_session.createDataFrame([expected_data], schema=expected_schema)
        # to_timestamp will not work for creation of test data, unix approach preferred
        .withColumn("time_example", F.from_unixtime(F.unix_timestamp("time_example")))
    )

    input_df = expected_df.drop("date_example")
    actual_df = assign_column_to_date_string(input_df, "date_example", "time_example")

    assert_df_equality(actual_df, expected_df)
