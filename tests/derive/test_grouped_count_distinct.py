from chispa import assert_df_equality

from cishouseholds.derive import grouped_count_distinct


def test_grouped_count_distinct(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("A", 2, 2),
            ("A", 3, 2),
            ("B", 1, 1),
            ("B", 1, 1),
            ("C", 1, 1),
        ],
        schema="""type string, col_to_count integer, distinct_count integer""",
    )

    input_df = expected_df.drop("distinct_count")

    actual_df = grouped_count_distinct(input_df, "distinct_count", "col_to_count", ["type"])

    assert_df_equality(actual_df, expected_df)
