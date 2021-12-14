from chispa import assert_df_equality

from cishouseholds.weights.edit import update_column


def test_update_column(spark_session):
    # df: DataFrame, lookup_df: DataFrame, column_name_to_update: str, join_on_columns: List[str]
    input_df = spark_session.createDataFrame(
        data=[
            ("A", 1, 1),
            ("B", 2, 0),
            ("C", 4, 1),
        ],
        schema="""
            col string,
            id integer,
            MATCHED_col integer
            """,
    )
    lookup_df = spark_session.createDataFrame(
        data=[
            (1, 2),
            (3, 4),
            (3, 6),
        ],
        schema="""
            id integer,
            col integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "C", 4),
            (0, "B", 2),
            (1, "A", 1),
        ],
        schema="""
            MATCHED_col integer,
            col string,
            id integer
            """,
    )
    output_df = update_column(df=input_df, lookup_df=lookup_df, column_name_to_update="col", join_on_columns=["id"])

    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
