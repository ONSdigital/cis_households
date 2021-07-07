import pytest
from chispa import assert_df_equality

from cishouseholds.derive import mean_across_columns


@pytest.mark.parametrize(
    "expected_data,expected_schema",
    [
        ((1, 2, 3, 2.0), "col1 integer, col2 integer, col3 integer"),
        ((1, 3, 2.0), "col1 integer, col2 integer"),
        ((2, 2.0), "col1 integer"),
        ((1, None, 3, 2.0), "col1 integer, col2 integer, col3 integer"),
        ((1, 0, 3, 2.0), "col1 integer, col2 integer, col3 integer"),
    ],
)
def test_mean(spark_session, expected_data, expected_schema):
    expected_df = spark_session.createDataFrame([expected_data], schema=expected_schema + ", mean double")
    input_df = expected_df.drop("mean")
    actual_df = mean_across_columns(input_df, "mean", input_df.columns)
    assert_df_equality(actual_df, expected_df)
