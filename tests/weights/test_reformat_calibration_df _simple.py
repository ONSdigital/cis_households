from chispa import assert_df_equality

from cishouseholds.weights.edit import reformat_calibration_df_simple


def test_reformat_calibration_df_simple(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("A", "B", 3),
            ("A", "B", 2),
            ("A", "B", 1),
            ("A", None, 5),
            ("A", None, 4),
        ],
        schema="""
            groupby_1 string,
            groupby_2 string,
            pivot integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[("P1A", 15), ("P2B", 6)],
        schema="""
            group string,
            population_total long
            """,
    )
    output_df = reformat_calibration_df_simple(
        df=input_df, population_column="pivot", groupby_columns=["groupby_1", "groupby_2"]
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
