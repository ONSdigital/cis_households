from chispa import assert_df_equality

from cishouseholds.derive import assign_column_from_mapped_list_key


def test_assign_column_from_mapped_list_key(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("a", 1),
            ("b", 1),
            ("c", 1),
            ("d", 2),
            ("e", 2),
            ("f", 2),
        ],
        schema="ref string, res integer",
    )
    output_df = assign_column_from_mapped_list_key(
        expected_df.drop("res"), "res", "ref", {1: ["a", "b", "c"], 2: ["d", "e", "f"]}
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
