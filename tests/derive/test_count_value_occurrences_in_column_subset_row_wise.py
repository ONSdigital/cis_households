from chispa import assert_df_equality

from cishouseholds.derive import count_value_occurrences_in_column_subset_row_wise


def test_count_value_occurrences_in_column_subset_row_wise(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 0, 0, 1),
            (0, 1, 0, 1),
            (1, None, 1, 2),
        ],
        schema="""
        primary_column integer,
        secondary_column integer,
        tertiary_column integer,
        count integer
        """,
    )
    output_df = count_value_occurrences_in_column_subset_row_wise(
        df=expected_df.drop("count"),
        column_name_to_assign="count",
        selection_columns=["primary_column", "secondary_column", "tertiary_column"],
        count_if_value=1,
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True)
