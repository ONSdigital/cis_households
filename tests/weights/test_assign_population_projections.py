from chispa import assert_df_equality

from cishouseholds.weights.derive import assign_population_projections


def test_assign_population_projections(spark_session):
    input_df1 = spark_session.createDataFrame(
        data=[
            (1, 1, 2, 3, 4),
            (2, 1, 3, 4, 5),
            (3, 2, 3, 3, 4),
        ],
        schema="""
            id integer,
            m1 integer,
            m2 integer,
            f1 integer,
            f2 integer
            """,
    )
    input_df2 = spark_session.createDataFrame(
        data=[
            (1, 2, 3, 3, 5),
            (2, 4, 5, 5, 7),
            (3, 2, 3, 3, 4),
        ],
        schema="""
            id integer,
            m1 integer,
            m2 integer,
            f1 integer,
            f2 integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 1.9166666666666665, 2.9166666666666665, 3.0, 4.916666666666666),
            (2, 3.75, 4.833333333333333, 4.916666666666666, 6.833333333333333),
            (3, 2.0, 3.0, 3.0, 4.0),
        ],
        schema="""
            id integer,
            m1 double,
            m2 double,
            f1 double,
            f2 double
            """,
    )
    output_df = assign_population_projections(
        current_projection_df=input_df1, previous_projection_df=input_df2, month=7, m_f_columns=["m1", "m2", "f1", "f2"]
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
