from chispa import assert_df_equality

from cishouseholds.derive import assign_ethnicity_white


def test_assign_ethnicity_white(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("White", "White"), ("Mixed", "Non-White")],
        schema="""ethnicity_group string, ethnicity_white string""",
    )
    output_df = assign_ethnicity_white(
        expected_df.drop("ethnicity_white"),
        ethnicity_group_column_name="ethnicity_group",
        column_name_to_assign="ethnicity_white",
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_row_order=True)
