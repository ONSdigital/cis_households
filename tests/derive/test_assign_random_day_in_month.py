import random

from chispa import assert_df_equality

from cishouseholds.derive import assign_random_day_in_month


def test_assign_random_day_in_month(spark_session):
    data = []
    n = 50
    for i in range(0, n):
        data.append((random.randint(2018, 2024), random.randint(1, 12)))
    expected_df = spark_session.createDataFrame(
        data=data,
        schema="year integer, month integer",
    )
    output_df = assign_random_day_in_month(
        df=expected_df.drop("result"), column_name_to_assign="result", year_column="year", month_column="month"
    )
    assert str(output_df.schema["result"].dataType) == "TimestampType"
