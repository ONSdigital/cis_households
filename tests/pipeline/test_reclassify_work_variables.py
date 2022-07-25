import pandas as pd
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.pipeline.high_level_transformations import reclassify_work_variables


@pytest.mark.integration
def test_reclassify_work_variables(spark_session):
    """Test that high level work variables reclassification is handled correctly"""

    test_data = [
        (
            0,
            "input",
            None,
            None,
            "WORKS FROM HOME",
            "ADMINSTRATOR",
            54,
            None,
            "Employed",
            "Employed and currently working",
            "Employed and currently working",
            None,
            None,
            None,
            None,
        ),
        (
            0,
            "expected",
            None,
            None,
            "ADMIN - WORKS FROM HOME",
            "ADMINSTRATOR",
            54,
            "Working from home",
            "Employed",
            "Employed and currently working",
            "Employed and currently working",
            True,
            False,
            False,
            False,
        ),
        (
            1,
            "input",
            None,
            None,
            "ASSISTANT SCHOOL TEACHER",
            "FURLOWED",
            70,
            "OFFICE",
            "Employed",
            "Employed and currently not working",
            "Employed and currently not working",
            None,
            None,
            None,
            None,
        ),
        (
            1,
            "expected",
            None,
            None,
            "ASSISTANT SCHOOL TEACHER",
            "FURLOWED",
            70,
            "OFFICE",
            "Furloughed (temporarily not working)",
            "Employed and currently not working",
            "Employed and currently not working",
            False,
            True,
            False,
            False,
        ),
    ]

    schema = [
        "row_id integer",
        "record_type string",
        "main_job string",
        "main_resp string",
        "work_main_job_title string",
        "work_main_job_role string",
        "age_at_visit integer",
        "work_location string",
        "work_status_v0 string",
        "work_status_v1 string",
        "work_status_v2 string",
        "work_location_edited boolean",
        "work_status_v0_edited boolean",
        "work_status_v1_edited boolean",
        "work_status_v2_edited boolean",
    ]

    expected_df = (
        spark_session.createDataFrame(test_data, schema=",".join(schema))
        .filter("record_type=='expected'")
        .drop("record_type")
    )

    input_df = (
        spark_session.createDataFrame(test_data, schema=",".join(schema))
        .filter("record_type=='input'")
        .drop("record_type", *[col for col in expected_df.columns if col.endswith("edited")])
    )

    actual_df = reclassify_work_variables(input_df)

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
