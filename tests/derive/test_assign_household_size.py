from chispa import assert_df_equality

from cishouseholds.derive import assign_household_size


def test_assign_household_size(spark_session):
    expected_df_with_preferred_count_col = spark_session.createDataFrame(
        data=[
            (1, "1"),
            (2, "2"),
            (6, "5+"),
        ],
        schema="preferred_count integer, result string",
    )
    expected_df_without_preferred_count_col = spark_session.createDataFrame(
        data=[
            (1, "1"),
            (2, "2"),
            (6, "5+"),
        ],
        schema="count integer, result string",
    )
    output_df1 = assign_household_size(
        df=expected_df_with_preferred_count_col.drop("result"),
        column_name_to_assign="result",
        household_size_group_column="preferred_count",
    )
    output_df2 = assign_household_size(
        df=expected_df_without_preferred_count_col.drop("result"),
        column_name_to_assign="result",
        household_participant_count_column="count",
    )
    assert_df_equality(output_df1, expected_df_with_preferred_count_col, ignore_nullable=True)
    assert_df_equality(output_df2, expected_df_without_preferred_count_col, ignore_nullable=True)
