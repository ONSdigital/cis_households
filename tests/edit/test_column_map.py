import pytest
from chispa import assert_df_equality

from cishouseholds.edit import assign_from_map


@pytest.mark.parametrize(
    "expected_data,expected_schema_types,expected_map",
    [
        ((1, 10), ("integer", "integer"), {1: 10}),
        (("1", "10"), ("string", "string"), {"1": "10"}),
        ((1, 1), ("integer", "integer"), {2: 20}),
        (("1", 1), ("string", "integer"), {"1": 1}),  # df.replace would fail this
        ((1, "1"), ("integer", "string"), {1: "1"}),
        (("1", None), ("string", "integer"), {"2": 2}),
    ],
)
def test_assign_from_map(spark_session, expected_data, expected_schema_types, expected_map):
    schema = f"key_column {expected_schema_types[0]}, value_column {expected_schema_types[1]}"
    expected_df = spark_session.createDataFrame(data=[expected_data], schema=schema)

    input_df = expected_df.drop("value_column")

    actual_df = assign_from_map(input_df, "value_column", "key_column", expected_map)

    assert_df_equality(actual_df, expected_df)
