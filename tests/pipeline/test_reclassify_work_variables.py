import pandas as pd
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql import types as t

from cishouseholds.pipeline.high_level_transformations import reclassify_work_variables


@pytest.fixture
def load_test_cases():
    # NOTE: school_year must be given as a string or null  in the csv. When we read into Spark Dataframe, we
    # cast into IntegerType(). The reason why we have this workaround is because when we read the csv (had
    # school_year been given as an integer with some missing values, Pandas will read it as a float type since
    # it cannot handle nulls in an integer column) - this will cause issues when you try to read into Spark
    # Dataframe as a Integer column.
    test_data = pd.read_csv("tests/pipeline/test_reclassify_work_variables/test-cases.csv")

    # if you want to test on individual records in the test-cases.csv file, then you can apply a filter on row_id below
    # in both expected & input data eg: query("record_type=='expected' and row_id==3")
    row_id = 1
    expected_data = test_data.query(f"record_type=='expected' and row_id==12").drop(columns=["record_type"])

    input_data = test_data.query(f"record_type=='input' and row_id==12").drop(
        columns=["record_type"] + [col for col in test_data.columns if "_hit_" in col]
    )

    return input_data, expected_data


@pytest.mark.integration
def test_reclassify_work_variables(spark_session, load_test_cases):
    """Test that high level work variables reclassification is handled correctly"""

    input_data, expected_data = load_test_cases

    input_schema = t.StructType(
        [
            t.StructField("row_id", t.FloatType()),
            t.StructField("rule", t.StringType()),
            t.StructField("survey_response_dataset_major_version", t.StringType()),
            t.StructField("work_main_job_title", t.StringType()),
            t.StructField("work_main_job_role", t.StringType()),
            t.StructField("age_at_visit", t.StringType()),
            t.StructField("school_year", t.StringType()),
            t.StructField("work_location", t.StringType()),
            t.StructField("work_status_v0", t.StringType()),
            t.StructField("work_status_v1", t.StringType()),
            t.StructField("work_status_v2", t.StringType()),
        ]
    )

    expected_df = (
        spark_session.createDataFrame(expected_data, schema=input_schema)
        .replace("NaN", None)
        .withColumn("school_year", F.col("school_year").cast("integer"))
    )

    input_df = (
        spark_session.createDataFrame(input_data, schema=input_schema)
        .replace("NaN", None)
        .withColumn("school_year", F.col("school_year").cast("integer"))
    )
    actual_df = reclassify_work_variables(input_df, spark_session=spark_session, drop_original_variables=True)
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=False,
    )
