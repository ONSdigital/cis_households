import random

from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_random_day_in_month


def test_assign_random_day_in_month(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            # fmt:off
            (1, 1, 2020),
            (2, 12, 1999),
            # fmt:on
        ],
        schema="""
            id integer,
            month integer,
            year integer
        """,
    )

    output_df = assign_random_day_in_month(
        df=input_df, column_name_to_assign="result", year_column="year", month_column="month"
    ).drop("month", "year")
    output_df = output_df.withColumn("month", F.month(F.col("result")))
    output_df = output_df.withColumn("year", F.year(F.col("result")))
    assert_df_equality(
        output_df.drop("result"),  # Day varies, so check that month and year parts are correct
        input_df,
        ignore_row_order=True,
    )
