import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_column_regex_match


@pytest.mark.parametrize(
    "expected_data",
    [("A1", True), ("AA", False), ("11", False), ("", False), ("?", False), (None, None)],
)
def test_derive_regex_match(spark_session, expected_data):

    expected_schema = "reference_column string, match boolean"
    expected_df = spark_session.createDataFrame([expected_data], schema=expected_schema)

    input_df = expected_df.drop("match")

    actual_df = assign_column_regex_match(input_df, "match", "reference_column", r"[a-zA-Z]+[0-9]+")

    assert_df_equality(actual_df, expected_df)
