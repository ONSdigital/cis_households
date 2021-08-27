import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.merge import assign_unique_identifier_column


def test_derive_unique_identifier_column(spark_session):

    expected_df = spark_session.createDataFrame(
        data=[
            ("1", "2021-08-01", 1),
            ("2", "2021-08-02", 3),
            ("2", "2021-08-01", 2),
            ("4", "2021-08-01", 5),
            ("4", "2021-08-02", 6),
            ("4", "2021-08-03", 7),
            ("3", "2021-08-02", 4),
        ],
        schema="id string, visit_date string, unique_id integer",
    ).withColumn("visit_date", F.to_date(F.col("visit_date")))

    input_df = expected_df.drop("unique_id")
    ordering_columns = ["id", "visit_date"]

    output_df = assign_unique_identifier_column(input_df, "unique_id", ordering_columns)

    assert_df_equality(
        output_df.orderBy(*ordering_columns), expected_df.orderBy(*ordering_columns), ignore_nullable=True
    )
