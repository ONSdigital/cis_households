import os

from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.impute import impute_date_by_k_nearest_neighbours


def test_impute_date_by_k_nearest_neighbours(spark_session):
    os.environ["deployment"] = "local"
    input_df = spark_session.createDataFrame(
        data=[
            # fmt:off
            (1,         '2020-01-01',       'A'),
            (2,         None,               'A'),
            # fmt:on
        ],
        schema="""
            id integer,
            date string,
            group string
        """,
    ).withColumn("date", F.to_timestamp(F.col("date")))

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt:off
            (1, 'A', None, None),
            (2, 'A', 1, 2020),
            # fmt:on
        ],
        schema="""
            id integer,
            group string,
            month integer,
            year integer
        """,
    )

    output_df = impute_date_by_k_nearest_neighbours(
        df=input_df,
        column_name_to_assign="date",
        reference_column="date",
        donor_group_columns=["group"],
        log_file_path="/",
    )
    output_df = output_df.withColumn("month", F.month(F.col("date")))
    output_df = output_df.withColumn("year", F.year(F.col("date")))
    assert_df_equality(
        output_df.drop("date"),  # Day part varies
        expected_df,
        ignore_row_order=True,
    )
