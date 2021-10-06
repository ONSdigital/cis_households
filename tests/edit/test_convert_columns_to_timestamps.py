import pyspark.sql.functions as F
import pytest
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

from cishouseholds.edit import convert_columns_to_timestamps


@pytest.mark.parametrize(
    "input_data, column_map",
    [
        (
            ("2022-01-08T07:51:35.000Z", "ONS123", "1966-07-30"),
            {"yyyy-MM-dd'T'HH:mm:ss.SSSS": ["time_example"],"yyyy-MM-dd": ["second_time_example"]},
        )
    ],
)
def test_convert_to_timestamp(spark_session, input_data, column_map):

    input_schema = "time_example string, ID string, second_time_example string"

    expected_schema = StructType(
        [
            StructField("time_example", TimestampType()),
            StructField("ID", StringType()),
            StructField("second_time_example", TimestampType()),
        ]
    )

    input_df = spark_session.createDataFrame([input_data], schema=input_schema)
    actual_df = convert_columns_to_timestamps(input_df, column_map)

    assert actual_df.schema == expected_schema

    contains_nulls = False

    for c in actual_df.columns:
        if len(actual_df.where(F.col(c).isNull()).limit(1).collect()) != 0:
            contains_nulls = True

    assert not contains_nulls
