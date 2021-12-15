import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.weights.derive import assign_sample_new_previous


def test_assign_sample_new_previous(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("20/07/2009", 4, "previous"),
            ("20/07/2004", 2, "previous"),
            ("20/07/2011", 2, "new"),
            ("20/07/2011", 2, "new"),
            ("20/07/2011", 1, "previous"),
        ],
        schema="""
            date string,
            batch integer,
            sample_new_previous string
            """,
    )
    output_df = assign_sample_new_previous(
        expected_df.drop("sample_new_previous"), "sample_new_previous", "date", "batch"
    )
    expected_df = expected_df.withColumn("date", F.to_timestamp(F.col("date"), format="dd/MM/yyyy"))
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
