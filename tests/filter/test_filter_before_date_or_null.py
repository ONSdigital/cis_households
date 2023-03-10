from chispa import assert_df_equality

from cishouseholds.filter import filter_before_date_or_null


def test_filter_before_date_or_null(spark_session):  # test funtion
    input_df = spark_session.createDataFrame(
        data=[
            (1, "1990-03-03"),  # filtered out
            (2, "2023-03-01"),
            (3, None),
            (4, "2023-01-01"),
        ],
        schema=["id", "date_col"],
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (2, "2023-03-01"),
            (3, None),
            (4, "2023-01-01"),
        ],
        schema=["id", "date_col"],
    )
    output_df = filter_before_date_or_null(df=input_df, date_column="date_col", min_date="2023-01-01")
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
