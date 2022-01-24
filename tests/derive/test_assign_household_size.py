from chispa import assert_df_equality

from cishouseholds.derive import assign_household_size


def test_assign_household_size(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "1", "1"),
            (2, None, "2"),
            (6, None, "5+"),
        ],
        schema="household_size integer, existing_group string, output_group string",
    )

    output_df1 = assign_household_size(
        df=expected_df.drop("output_group"),
        column_name_to_assign="output_group",
        household_participant_count_column="household_size",
        existing_group_column="existing_group",
    )
    assert_df_equality(output_df1, expected_df)
