from chispa import assert_df_equality
from pyspark.sql import DataFrame

from cishouseholds.expressions import first_sorted_val_row_wise
from cishouseholds.expressions import last_sorted_val_row_wise

# import pyspark.sql import SparkSession


def test_first_sorted_val_row_wise(spark_session):
    data = [
        # fmt:off
            (2,4,1,1),
            (2,2,2,2),
            (69,-1,53,-1),
        # fmt:on
    ]

    schema = """
            A integer,
            B integer,
            C integer,
            first integer
            """

    expected_df: DataFrame = spark_session.createDataFrame(data=data, schema=schema)
    input_df = expected_df.drop("first")
    output_df = input_df.withColumn("first", first_sorted_val_row_wise(["A", "B", "C"]))

    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_last_sorted_val_row_wise(spark_session):
    data = [
        # fmt:off
            (2,4,1,4),
            (2,2,2,2),
            (69,-1,53,69),
        # fmt:on
    ]

    schema = """
            A integer,
            B integer,
            C integer,
            last integer
            """

    expected_df: DataFrame = spark_session.createDataFrame(data=data, schema=schema)
    input_df = expected_df.drop("last")
    output_df = input_df.withColumn("last", last_sorted_val_row_wise(["A", "B", "C"]))

    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
