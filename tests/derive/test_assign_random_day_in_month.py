from chispa import assert_df_equality

from cishouseholds.derive import assign_random_day_in_month


def test_assign_random_day_in_month(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (2019, 6),
            (2020, 8),
            (2020, 4),
        ],
        schema="year integer, month integer",
    )
    output_df = assign_random_day_in_month(
        df=expected_df.drop("result"), column_name_to_assign="result", year_column="year", month_column="month"
    )
    types = {}
    for col, type in output_df.dtypes:
        types[col] = type
    assert types["result"] == "timestamp"
