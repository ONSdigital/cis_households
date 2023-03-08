from chispa import assert_df_equality

from cishouseholds.derive import assign_columns_from_array


def test_assign_columns_from_array(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, ["a", "b", "c"]),
            (2, ["a", "c"]),
        ],
        schema=["id", "arr"],
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, ["a", "b", "c"], True, True, True),
            (2, ["a", "c"], True, False, True),
        ],
        schema=["id", "arr", "test_a", "test_b", "test_c"],
    )
    output_df = assign_columns_from_array(
        df=input_df, array_column_name="arr", prefix="test", true_false_values=[True, False]
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
