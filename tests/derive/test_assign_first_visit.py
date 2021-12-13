from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_first_visit


def test_assign_first_visit(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(1, "2020-02-06", "2019-12-12"), (1, "2019-12-12", "2019-12-12"), (2, "2020-01-22", "2020-01-22")],
        schema="id integer, visit_date string, result string",
    )
    for col in ["visit_date", "result"]:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col, format="yyyy-MM-dd"))
    output_df = assign_first_visit(
        df=expected_df.drop("result"), column_name_to_assign="result", id_column="id", visit_date_column="visit_date"
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
