from chispa import assert_df_equality
from pyspark.sql import DataFrame

from cishouseholds.expressions import count_occurrence_in_row

# import pyspark.sql import SparkSession


def test_count_occurrence_in_row(spark_session):
    data = [
        # fmt:off
            ("Yes","No","No",1),
            ("Yes","Yes","No",2),
            ("Yes",None,None,1),
        # fmt:on
    ]

    schema = """
            A string,
            B string,
            C string,
            count integer
            """

    expected_df: DataFrame = spark_session.createDataFrame(data=data, schema=schema)
    input_df = expected_df.drop("count")
    output_df = input_df.withColumn("count", count_occurrence_in_row(["A", "B", "C"], "Yes"))

    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
