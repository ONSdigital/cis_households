import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.derive import assign_date_from_filename


def test_csv_filename_with_date_and_time_appends_date_and_time(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("xyz_20200511_052100.csv", "2020-05-11 05:21:00"),
        ],
        schema="file string, datetime string",
    )
    expected_df = expected_df.withColumn("datetime", F.to_timestamp(F.col("datetime"), format="yyyy-MM-dd HH:mm:ss"))
    output_df = assign_date_from_filename(expected_df.drop("datetime"), "datetime", "file")
    assert_df_equality(output_df, expected_df, ignore_nullable=True)


def test_csv_filename_with_date_appends_date(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("xyz_20200511.csv", "2020-05-11 00:00:00"),
        ],
        schema="file string, datetime string",
    )
    expected_df = expected_df.withColumn("datetime", F.to_timestamp(F.col("datetime"), format="yyyy-MM-dd HH:mm:ss"))
    output_df = assign_date_from_filename(expected_df.drop("datetime"), "datetime", "file")
    assert_df_equality(output_df, expected_df, ignore_nullable=True)


def test_txt_filename_with_date_and_time_appends_date_and_time(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("ONSE_CIS_Digital_v1_0_responses_20200511_000000.txt", "2020-05-11 00:00:00"),
        ],
        schema="file string, datetime string",
    )
    expected_df = expected_df.withColumn("datetime", F.to_timestamp(F.col("datetime"), format="yyyy-MM-dd HH:mm:ss"))
    output_df = assign_date_from_filename(expected_df.drop("datetime"), "datetime", "file")
    assert_df_equality(output_df, expected_df, ignore_nullable=True)


def test_json_filename_with_date_and_time_appends_date_and_time(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("test_739_2023-04-05T130316.json", "2023-04-05 13:03:16"),
        ],
        schema="file string, datetime string",
    )
    expected_df = expected_df.withColumn("datetime", F.to_timestamp(F.col("datetime"), format="yyyy-MM-dd HH:mm:ss"))
    output_df = assign_date_from_filename(expected_df.drop("datetime"), "datetime", "file")
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
