from chispa import assert_df_equality

from cishouseholds.derive import assign_true_if_any


def test_assign_true_if_either(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("false", "false", "false"),
            ("false", "true", "true"),
            ("true", "true", "true"),
            ("true", None, "true"),
        ],
        schema="ref1 string, ref2 string, result string",
    )
    output_df = assign_true_if_any(
        df=expected_df.drop("result"),
        column_name_to_assign="result",
        reference_columns=["ref1", "ref2"],
        true_false_values=["true", "false"],
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
