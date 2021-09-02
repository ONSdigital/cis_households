import pytest
from chispa import assert_df_equality

from cishouseholds.merge import assign_absolute_offset


@pytest.mark.parametrize(
    "expected_data, offset, col_type",
    [
        ((10, 9), 1, "integer"),
        ((1, 2), 3, "integer"),
        ((10.5, 9.5), 1, "float"),
        ((5.0, 5.0), 10, "float"),
        ((None, None), 1, "integer"),
        ((10, 10), 0, "integer"),
    ],
)
def test_absolute_offset(spark_session, expected_data, offset, col_type):

    expected_schema = f"interval {col_type}, absolute_offset {col_type}"

    expected_df = spark_session.createDataFrame(data=[expected_data], schema=expected_schema)

    input_df = expected_df.drop("absolute_offset")

    output_df = assign_absolute_offset(input_df, "absolute_offset", "interval", offset=offset)

    assert_df_equality(output_df, expected_df)
