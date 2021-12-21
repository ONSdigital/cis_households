from chispa import assert_df_equality

from cishouseholds.derive import assign_isin_list


def test_assign_isin_list(spark_session):

    column_names = "pattern string, outcome integer"

    expected_df = spark_session.createDataFrame(
        data=[("OR only", 1), ("N only", 1), ("OR+N", 0), ("OR+N+S", 0), (None, None), ("S only", 1)],
        schema=column_names,
    )

    value_list = ["OR only", "N only", "S only"]

    input_df = expected_df.drop("outcome")

    actual_df = assign_isin_list(input_df, "outcome", "pattern", value_list, [1, 0])

    assert_df_equality(actual_df, expected_df)
