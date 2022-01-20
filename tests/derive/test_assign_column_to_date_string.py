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


def test_assign_column_to_date_string_specify_format(spark_session):
    schema_input_df = """
                        time_column string,
                        time_column_string string
                    """
    data_input_df = [
        ("2020-05-19 13:38:00", "19may2020 13:38:00"),
    ]
    expected_df = spark_session.createDataFrame(data_input_df, schema=schema_input_df)

    input_df = expected_df.drop("time_column_string")
    output_df = assign_column_to_date_string(
        df=input_df,
        column_name_to_assign="time_column_string",
        reference_column="time_column",
        time_format="ddMMMyyyy HH:mm:ss",
        lower_case=True,
    )
    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
