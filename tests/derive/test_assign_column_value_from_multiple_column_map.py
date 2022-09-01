from chispa import assert_df_equality

from cishouseholds.derive import assign_column_value_from_multiple_column_map


def test_assign_column_value_from_multiple_column_map(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (None, 6, 2, 1),
            (1, 1, None, None),
            (2, 3, 3, None),
            (3, 2, 5, 4),
        ],
        schema="result integer, s1 integer, s2 integer, s3 integer",
    )
    output_df = assign_column_value_from_multiple_column_map(
        expected_df.drop("result"),
        "result",
        [[1, [1, None, None]], [2, [3, 3, None]], [3, [2, 5, [3, 4]]]],
        ["s1", "s2", "s3"],
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
