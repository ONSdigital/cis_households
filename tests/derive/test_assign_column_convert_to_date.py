import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_column_convert_to_date


@pytest.mark.parametrize(
    "expected_data",
    [
        (F.to_timestamp("1966-07-30 15:00:00", "yyyy-MM-dd HH:mm:ss"), F.to_date("1966-07-30", "yyyy-MM-dd")),
        (F.lit(None), F.lit(None)),
    ],
)
def test_convert_to_date(spark_session, expected_data):

    expected_schema = "time_example timestamp, date_example date"

    expected_df = spark_session.createDataFrame([expected_data], schema=expected_schema)
    input_df = expected_df.drop("date_example")
    actual_df = assign_column_convert_to_date(input_df, "time_example", "date_example")
    assert_df_equality(actual_df, expected_df)
