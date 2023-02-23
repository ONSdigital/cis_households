from chispa import assert_df_equality
from pyspark.sql import functions as F


def test_assign_default_vaccine_date(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(1, "2020-02-15", 1), (2, "2019-12-12", 0), (3, "2020-01-11", 0), (4, "2020-01-01", 1)],
        schema="id integer, cis_covid_vaccine_date string, default_vaccine_date integer",
    )
    for col in ["cis_covid_vaccine_date"]:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col, format="yyyy-MM-dd"))
    output_df = expected_df.drop("result").withColumn(
        "default_vaccine_date", F.when(F.dayofmonth("cis_covid_vaccine_date").isin([1, 15]), 1).otherwise(0)
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=False, ignore_column_order=False)
