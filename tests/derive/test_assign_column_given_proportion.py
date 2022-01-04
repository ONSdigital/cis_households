from chispa import assert_df_equality

from cishouseholds.derive import assign_column_given_proportion


def test_assign_column_given_proportion(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(1, 1, 0), (1, 0, 0), (1, 0, 0), (1, 0, 0), (2, None, 0), (3, 1, 1), (4, 1, 1), (4, None, 1)],
        schema="""
        id integer,
        col integer,
        result integer
        """,
    )

    input_df = expected_df.drop("result")

    output_df = assign_column_given_proportion(
        df=input_df,
        column_name_to_assign="result",
        groupby_column="id",
        reference_columns=["col"],
        count_if=[1],
        true_false_values=[1, 0],
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
