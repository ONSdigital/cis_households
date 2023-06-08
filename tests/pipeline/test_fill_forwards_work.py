import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.pipeline.job_transformations import fill_forwards


def test_filling_forwards(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, "01-01-2022", "abc", "dce", "hospitality", None, "primary", None),
            (2, "01-01-2022", "abc", "dce", "Healthcare", None, "secondary", "Yes"),
            (3, "01-01-2022", "abc", "dce", "Health care", None, "other", "No"),
            (4, "01-01-2022", "abc", "dce", "manufacturing", None, None, None),
            (5, "01-01-2022", "abc", "dce", "Social care", None, None, "Yes"),
            (6, "01-01-2022", "abc", "dce", "finance", None, "secondary", "No"),
        ],
        schema="""participant_id integer, visit_datetime string, work_main_job_title string, work_main_job_role string,
                  work_sector string, work_sector_other string, work_health_care_area string,
                  work_nursing_or_residential_care_home string""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "01-01-2022", "abc", "dce", "hospitality", None, None, None),
            (2, "01-01-2022", "abc", "dce", "Healthcare", None, "secondary", "Yes"),
            (3, "01-01-2022", "abc", "dce", "Health care", None, "other", "No"),
            (4, "01-01-2022", "abc", "dce", "manufacturing", None, None, None),
            (5, "01-01-2022", "abc", "dce", "Social care", None, None, "Yes"),
            (6, "01-01-2022", "abc", "dce", "finance", None, None, None),
        ],
        schema="""participant_id integer, visit_datetime string, work_main_job_title string, work_main_job_role string,
                  work_sector string, work_sector_other string, work_health_care_area string,
                  work_nursing_or_residential_care_home string""",
    )

    output_df = fill_forwards(input_df)
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
