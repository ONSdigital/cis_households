from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_from_map


def test_assign_regex_from_map(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("AB", "A"["A", "B"]), ("A", "B"["A", "B"]), ("C", "D", []), ("A", "C", ["A"])],
        schema=["colA", "colB", "result"],
    )

    map = {"A": "A", "B": "B"}

    output_df = assign_regex_from_map(expected_df.drop("result"), "result", ["colA", "colB"], map)
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
