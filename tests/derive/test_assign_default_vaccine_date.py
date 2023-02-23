from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_default_date_flag


def test_assign_default_vaccine_date(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(1, "2020-02-15", 1), (2, "2019-12-12", 0), (3, "2020-01-11", 0), (4, "2020-01-01", 1)],
        schema="id integer, cis_covid_vaccine_date string, default_cis_covid_vaccine_date integer",
    )
    for col in ["cis_covid_vaccine_date"]:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col, format="yyyy-MM-dd"))
    output_df = assign_default_date_flag(
        expected_df.drop("default_cis_covid_vaccine_date"), "cis_covid_vaccine_date", default_days=[1, 15]
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=False, ignore_column_order=False)
