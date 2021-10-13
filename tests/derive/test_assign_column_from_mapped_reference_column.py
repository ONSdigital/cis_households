from chispa import assert_df_equality

from cishouseholds.derive import assign_column_from_mapped_reference_column


def test_assign_column_from_mapped_reference_column(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "Yes"),
            (8, None),
            (-9, "<=15y"),
            (0, "No"),
        ],
        schema="work integer, facing string",
    )
    expected_df2 = spark_session.createDataFrame(
        data=[
            (1, "Yes"),
            (8, ">=75y"),
            (-9, "<=15y"),
            (0, "No"),
        ],
        schema="work integer, facing string",
    )
    output_df1 = assign_column_from_mapped_reference_column(
        expected_df.drop("facing"), "work", "facing", {-9: "<=15y", -8: ">=75y", 0: "No", 1: "Yes"}, False
    )
    output_df2 = assign_column_from_mapped_reference_column(
        expected_df2, "work", "facing", {-9: "<=15y", -8: ">=75y", 0: "No", 1: "Yes"}, True
    )
    assert_df_equality(output_df1, expected_df)
    assert_df_equality(output_df2, expected_df2)
