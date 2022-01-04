import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.pipeline.post_merge_processing import filter_response_records


def test_filter_response_records(spark_session):
    df_schema = """
    survey_response_source_file string,
    file_date string,
    visit_date string,
    swab_sample_barcode string,
    blood_sample_barcode string
    """
    input_df_schema = """
    survey_response_source_file string,
    visit_date string,
    swab_sample_barcode string,
    blood_sample_barcode string
    """
    expected_data1 = [
        ("abc_def_20210621.csv", "2021-06-21", "2021-01-01", "ABC", "DEF"),
        ("abc_def_20211211.csv", "2021-12-11", "2021-01-01", None, None),
        ("abc_def_20210621.csv", "2021-06-21", "2021-12-30", None, "DEF"),
    ]
    expected_df1 = spark_session.createDataFrame(expected_data1, schema=df_schema)

    expected_data2 = [
        ("abc_def_20211211.csv", "2021-12-11", "2021-12-30", None, None),
        ("abc_def_20211211.csv", "2021-12-11", "2022-01-01", None, None),
    ]
    expected_df2 = spark_session.createDataFrame(expected_data2, schema=df_schema)

    input_data = [
        ("abc_def_20211211.csv", "2021-12-30", None, None),
        ("abc_def_20210621.csv", "2021-01-01", "ABC", "DEF"),
        ("abc_def_20211211.csv", "2021-01-01", None, None),
        ("abc_def_20210621.csv", "2021-12-30", None, "DEF"),
        ("abc_def_20211211.csv", "2022-01-01", None, None),
    ]

    input_df = spark_session.createDataFrame(input_data, schema=input_df_schema)

    expected_df1 = expected_df1.withColumn(
        "file_date", F.to_timestamp(F.col("file_date"), format="yyyy-MM-dd")
    ).withColumn("visit_date", F.to_timestamp(F.col("visit_date"), format="yyyy-MM-dd"))
    expected_df2 = expected_df2.withColumn(
        "file_date", F.to_timestamp(F.col("file_date"), format="yyyy-MM-dd")
    ).withColumn("visit_date", F.to_timestamp(F.col("visit_date"), format="yyyy-MM-dd"))
    input_df = input_df.withColumn("visit_date", F.to_timestamp(F.col("visit_date"), format="yyyy-MM-dd"))

    output_df1, output_df2 = filter_response_records(input_df, "visit_date")
    assert_df_equality(output_df1, expected_df1, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
    assert_df_equality(output_df2, expected_df2, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
