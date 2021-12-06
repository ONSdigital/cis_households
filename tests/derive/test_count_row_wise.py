from chispa import assert_df_equality

from cishouseholds.derive import count_true_row_wise


def test_assign_column_from_coalesce(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 0, 1),
            (None, 1, 1),
            (None, None, None),
        ],
        schema="""
        primary_column integer,
        secondary_column integer,
        coalesced_column integer
        """,
    )

    input_df = expected_df.drop("coalesced_column")

    output_df = count_true_row_wise(input_df, "count", ["primary_column", "secondary_column"])
    output_df.show()

    assert_df_equality(output_df, expected_df, ignore_nullable=True)
