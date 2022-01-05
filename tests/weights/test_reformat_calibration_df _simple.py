from chispa import assert_df_equality

from cishouseholds.weights.edit import reformat_calibration_df_simple


def test_reformat_calibration_df_simple(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, 2, 3),
            (2, 2, 2),
            (2, 3, 1),
            (2, None, 5),
            (1, None, 4),
        ],
        schema="""
            p1_group integer,
            p2_group integer,
            pivot integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[("P11", 7), ("P12", 8), ("P22", 5), ("P23", 1)],
        schema="""
            group string,
            population_total long
            """,
    )
    output_df = reformat_calibration_df_simple(
        df=input_df, population_column="pivot", groupby_columns=["p1_group", "p2_group"]
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
