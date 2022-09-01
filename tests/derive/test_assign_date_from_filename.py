import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.derive import assign_date_from_filename


def test_assign_date_from_filename(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("xyz_20200511_052100.csv", "2020-05-11 05:21:00"),
            ("xyz_20200511.csv", "2020-05-11 00:00:00"),
        ],
        schema="file string, datetime string",
    )
    expected_df = expected_df.withColumn("datetime", F.to_timestamp(F.col("datetime"), format="yyyy-MM-dd HH:mm"))
    output_df = assign_date_from_filename(expected_df.drop("datetime"), "datetime", "file")
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
