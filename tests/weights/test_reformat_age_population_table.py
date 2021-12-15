from chispa import assert_df_equality

from cishouseholds.weights.edit import reformat_age_population_table


def test_reformat_age_population_table(spark_session):
    # df: DataFrame, lookup_df: DataFrame, column_name_to_update: str, join_on_columns: List[str]
    input_df = spark_session.createDataFrame(
        data=[
            (1, "f1A", "m1A", "f1B", "m1B"),
            (2, "f2A", "m2A", "f2B", "m2B"),
            (3, "f3A", "m3A", "f3B", "m3B"),
        ],
        schema="""
            id integer,
            f1 string,
            m1 string,
            f2 string,
            m2 string
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "m", 1, "m1A"),
            (1, "m", 2, "m1B"),
            (2, "m", 1, "m2A"),
            (2, "m", 2, "m2B"),
            (3, "m", 1, "m3A"),
            (3, "m", 2, "m3B"),
            (1, "f", 1, "f1A"),
            (1, "f", 2, "f1B"),
            (2, "f", 1, "f2A"),
            (2, "f", 2, "f2B"),
            (3, "f", 1, "f3A"),
            (3, "f", 2, "f3B"),
        ],
        schema="""
            id integer,
            sex string,
            age integer,
            population string
            """,
    )
    output_df = reformat_age_population_table(df=input_df, m_f_columns=["f1", "m1", "m2", "f2"])
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
