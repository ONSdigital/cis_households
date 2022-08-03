import pandas as pd
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql import types as t

from cishouseholds.pipeline.high_level_transformations import reclassify_work_variables

# tests that break & need more review: [5, ]


@pytest.fixture
def load_test_cases():
    test_data = pd.read_csv("tests/pipeline/test_reclassify_work_variables/test-cases.csv")

    expected_data = test_data.query("record_type=='expected' and row_id==13").drop(columns=["record_type"])

    input_data = test_data.query("record_type=='input' and row_id==13").drop(
        columns=["record_type"] + [col for col in test_data.columns if "_hit_" in col]
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
            t.StructField("work_location_hit_working_from_home", t.BooleanType(), False),
            t.StructField("work_status_v0_hit_self_employed", t.BooleanType(), False),
            t.StructField("work_status_v1_hit_self_employed_v1_a", t.BooleanType()),
            t.StructField("work_status_v1_hit_self_employed_v1_b", t.BooleanType()),
            t.StructField("work_status_v2_hit_self_employed_v2_a", t.BooleanType()),
            t.StructField("work_status_v2_hit_self_employed_v2_b", t.BooleanType()),
            t.StructField("work_status_v0_hit_student_v0", t.BooleanType()),
            t.StructField("work_status_v1_hit_status_student_v1", t.BooleanType()),
            t.StructField("work_status_v2_hit_student_v2_a", t.BooleanType()),
            t.StructField("work_status_v2_hit_student_v2_b", t.BooleanType()),
            t.StructField("work_status_v2_hit_student_v2_c", t.BooleanType()),
            t.StructField("work_status_v0_hit_retired", t.BooleanType()),
            t.StructField("work_status_v1_hit_retired", t.BooleanType()),
            t.StructField("work_status_v2_hit_retired", t.BooleanType()),
            t.StructField("work_status_v0_hit_not_working_v0", t.BooleanType()),
            t.StructField("work_status_v1_hit_not_working_v1_a", t.BooleanType()),
            t.StructField("work_status_v1_hit_not_working_v1_b", t.BooleanType()),
            t.StructField("work_status_v2_hit_not_working_v2_a", t.BooleanType()),
            t.StructField("work_status_v2_hit_not_working_v2_b", t.BooleanType()),
            t.StructField("work_status_v0_hit_furlough_v0", t.BooleanType()),
            t.StructField("work_status_v1_hit_furlough_v1_a", t.BooleanType()),
            t.StructField("work_status_v1_hit_furlough_v1_b", t.BooleanType()),
            t.StructField("work_status_v2_hit_furlough_v2_a", t.BooleanType()),
            t.StructField("work_status_v2_hit_furlough_v2_b", t.BooleanType()),
            t.StructField("work_location_hit_general", t.BooleanType()),
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
