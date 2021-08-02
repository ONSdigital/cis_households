import pytest
from chispa import assert_df_equality

from cishouseholds.edit import assign_null_if_insufficient


@pytest.mark.parametrize(
    "expected_data",
    [(1, "sufficient", 1), (0, "sufficient", 0), (1, "Insufficient sample", 1), (0, "Insufficient sample", None)],
)
def test_assign_bloods_insufficient(spark_session, expected_data):
    schema = "first_reference integer, second_reference string, edited integer"
    expected_df = spark_session.createDataFrame(data=[expected_data], schema=schema)

    input_df = expected_df.drop("edited")
    actual_df = assign_null_if_insufficient(input_df, "edited", "first_reference", "second_reference")
    assert_df_equality(actual_df, expected_df)
