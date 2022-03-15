from chispa import assert_df_equality

from cishouseholds.weights.edit import reformat_age_population_table


def test_reformat_age_population_table(spark_session):
    # df: DataFrame, lookup_df: DataFrame, column_name_to_update: str, join_on_columns: List[str]
    input_df = spark_session.createDataFrame(
        data=[
            (1, 12, 10, 11, 15),
            (2, 17, 2, 15, 9),
            (3, 62, 1, 0, 5),
        ],
        schema="""
            id integer,
            f1 integer,
            m1 integer,
            f2 integer,
            m2 integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "m", 1, 10),
            (1, "m", 2, 15),
            (2, "m", 1, 2),
            (2, "m", 2, 9),
            (3, "m", 1, 1),
            (3, "m", 2, 5),
            (1, "f", 1, 12),
            (1, "f", 2, 11),
            (2, "f", 1, 17),
            (2, "f", 2, 15),
            (3, "f", 1, 62),
            (3, "f", 2, 0),
        ],
        schema="""
            id integer,
            sex string,
            age integer,
            population integer
            """,
    )
    output_df = reformat_age_population_table(df=input_df, m_f_columns=["f1", "m1", "m2", "f2"])
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
