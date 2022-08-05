import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import transform_cis_soc_data


def test_transform_cis_soc_data(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("job title 1", "job1", 5),
            ("job title 1", "job1", 5),
            ("job title 2", "job2", 6),
            ("job title 2", "job2", 66),
            ("job title 2", "job2", 77),
            ("job title 2", "job2", "un"),
            ("job title 3", "job3", "un"),
            (None, "job3", "un"),
            (None, None, None),
        ],
        schema="work_main_job_title string, work_main_job_role string, standard_occupational_classification_code string",
    )
    expected_df = spark_session.createDataFrame(
        data=[("job title 1", "JOB1", 5), ("job title 2", "JOB2", 66), ("job title 2", "JOB2", 77)],
        schema="work_main_job_title string, work_main_job_role string, standard_occupational_classification_code string",
    )
    # test mapping functionality with complete map off
    output_df = transform_cis_soc_data(input_df, ["work_main_job_role"])

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
