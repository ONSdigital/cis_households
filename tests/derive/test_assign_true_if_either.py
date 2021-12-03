from chispa import assert_df_equality

from cishouseholds.derive import assign_true_if_either


def test_assign_true_if_either(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("false", "false", "false"),
            ("false", "true", "true"),
            ("true", "true", "true"),
        ],
        schema="ref1 string, ref2 string, result string",
    )
    output_df = assign_true_if_either(
        df=expected_df.drop("result"),
        column_name_to_assign="result",
        reference_column2="ref2",
        reference_column1="ref1",
        true_false_values=["true", "false"],
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
