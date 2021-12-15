from chispa import assert_df_equality

from cishouseholds.weights.edit import reformat_calibration_df


def test_reformat_calibration_df(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("A", "B", 3),
            ("A", "B", 2),
            ("A", "B", 1),
            ("A", None, 5),
            ("A", None, 4),
        ],
        schema="""
            groupby1 string,
            groupby2 string,
            pivot integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (15, 6),
        ],
        schema="""
            PA integer,
            PB integer
            """,
    )
    output_df = reformat_calibration_df(df=input_df, pivot_column="pivot", groupby_columns=["groupby1", "groupby2"])
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
