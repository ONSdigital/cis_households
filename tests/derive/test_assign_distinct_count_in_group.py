from chispa import assert_df_equality

from cishouseholds.derive import assign_distinct_count_in_group


def test_assign_distinct_count_in_group(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "1", 1),
            ("B", "2", 2),
            ("B", "2", 2),
            ("C", "2", 2),
            ("D", "3", 3),
            ("E", "3", 3),
            ("F", "3", 3),
        ],
        schema="id string, group string, outcome integer",
    )
    output_df = assign_distinct_count_in_group(expected_df.drop("outcome"), "outcome", ["id"], ["group"])
    assert_df_equality(output_df, expected_df)
