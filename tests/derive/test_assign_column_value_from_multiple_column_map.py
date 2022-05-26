from chispa import assert_df_equality

from cishouseholds.derive import assign_column_value_from_multiple_column_map


def test_assign_column_value_from_multiple_column_map(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 6, 2, 1),
            # (1, None, None, 2),
            # (2, 3, None, None)
        ],
        schema="result integer, s1 integer, s2 integer, s3 integer",
    )
    expected_df.show()
    output_df = assign_column_value_from_multiple_column_map(
        expected_df.drop("result"), "result", {"result": {"s1": 1, "s2": None, "s3": None}}
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
