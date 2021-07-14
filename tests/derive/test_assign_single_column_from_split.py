import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_single_column_from_split


@pytest.mark.parametrize(
    "expected_data, item_number",
    [
        (("AAA BBBB", "AAA"), 0),
        (("AAA BBBB", "BBBB"), 1),
        (("AAA", "AAA"), 0),
        (("AAA", None), 1),
    ],
)
def test_assign_single_column_from_split(spark_session, expected_data, item_number):
    schema = "reference string, item string"
    expected_df = spark_session.createDataFrame(data=[expected_data], schema=schema)

    input_df = expected_df.drop("item")
    actual_df = assign_single_column_from_split(input_df, "item", "reference", item_number=item_number)
    assert_df_equality(actual_df, expected_df)
