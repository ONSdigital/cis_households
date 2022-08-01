import pandas as pd
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql import types as t

from cishouseholds.pipeline.high_level_transformations import reclassify_work_variables


@pytest.fixture
def load_test_cases():
    test_data = pd.read_csv("tests/pipeline/test_reclassify_work_variables/test-cases.csv")

    expected_data = test_data.query("record_type=='expected' and row_id==1").drop(columns=["record_type"])

    input_data = test_data.query("record_type=='input' and row_id==1").drop(
        columns=["record_type"] + [col for col in test_data.columns if col.endswith("edited")]
    )

    return input_data, expected_data


@pytest.mark.integration
def test_reclassify_work_variables(spark_session, load_test_cases):
    """Test that high level work variables reclassification is handled correctly"""

    input_data, expected_data = load_test_cases

    input_schema = t.StructType(
        [
            t.StructField("row_id", t.IntegerType()),
            t.StructField("rule", t.StringType()),
            t.StructField("main_job", t.StringType()),
            t.StructField("main_resp", t.StringType()),
            t.StructField("work_main_job_title", t.StringType()),
            t.StructField("work_main_job_role", t.StringType()),
            t.StructField("age_at_visit", t.IntegerType()),
            t.StructField("work_location", t.StringType()),
            t.StructField("work_status_v0", t.StringType()),
            t.StructField("work_status_v1", t.StringType()),
            t.StructField("work_status_v2", t.StringType()),
        ]
    )

    expected_schema = t.StructType(
        [
            t.StructField("row_id", t.IntegerType()),
            t.StructField("rule", t.StringType()),
            t.StructField("main_job", t.StringType()),
            t.StructField("main_resp", t.StringType()),
            t.StructField("work_main_job_title", t.StringType()),
            t.StructField("work_main_job_role", t.StringType()),
            t.StructField("age_at_visit", t.IntegerType()),
            t.StructField("work_location", t.StringType()),
            t.StructField("work_status_v0", t.StringType()),
            t.StructField("work_status_v1", t.StringType()),
            t.StructField("work_status_v2", t.StringType()),
            t.StructField("work_location_edited", t.BooleanType(), False),
            t.StructField("work_status_v0_edited", t.BooleanType()),
            t.StructField("work_status_v1_edited", t.BooleanType()),
            t.StructField("work_status_v2_edited", t.BooleanType()),
        ]
    )

    expected_df = spark_session.createDataFrame(expected_data, schema=expected_schema).replace("NaN", None)

    input_df = spark_session.createDataFrame(input_data, schema=input_schema).replace("NaN", None)

    actual_df = reclassify_work_variables(input_df, spark_session=spark_session)

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=False,
        ignore_nullable=False,
    )
