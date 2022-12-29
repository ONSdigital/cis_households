from chispa import assert_df_equality

from cishouseholds.derive import flag_records_for_work_location_null
from cishouseholds.derive import flag_records_for_work_location_student


def test_flag_records_for_work_location_null(spark_session):
    """Test flag_records_for_work_location_null function correctly flags the records"""

    test_cases = [
        (
            None,
            "Student",
            "5y and older in full-time education",
            "4-5y and older at school/home-school",
            "Student",
            "Go to School",
            True,
            3,
        ),
        (
            "Office",
            "Employed",
            "Employed and currently working",
            "Employed and currently working",
            None,
            None,
            False,
            3,
        ),
        (
            "Office",
            "Furloughed (temporarily not working)",
            "Employed and currently not working",
            "Employed and currently not working",
            None,
            None,
            True,
            3,
        ),
        (
            "Office",
            "Not working (unemployed, retired, long-term sick etc.)",
            "Not working and not looking for work",
            "Not working and not looking for work",
            None,
            None,
            True,
            3,
        ),
        (
            "Office",
            "Student",
            "5y and older in full-time education",
            "4-5y and older at school/home-school",
            None,
            None,
            True,
            3,
        ),
        (
            "Office",
            "Self-Employed",
            "Self-employed and currently working",
            "Self-employed and currently working",
            None,
            None,
            False,
            3,
        ),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="work_location string, work_status_v0 string, work_status_v1 string, work_status_v2 string, work_main_job_title string, work_main_job_role string, actual_flag boolean, survey_response_major_dataset_version int",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_work_location_null())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_work_location_student(spark_session):
    """Test flag_records_for_work_location_student function correctly flags the records"""

    test_cases = [
        (16, "Employed", "Employed and currently working", "Employed and currently working", False, 0),
        (15, "Student", "5y and older in full-time education", "4-5y and older at school/home-school", True, 0),
        (
            19,
            "Furloughed (temporarily not working)",
            "Employed and currently not working",
            "Employed and currently not working",
            False,
            0,
        ),
        (24, "Employed", "Employed and currently working", "Employed and currently working", False, 0),
        (65, "Retired", "Retired", "Retired", False, 0),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="age_at_visit int, work_status_v0 string, work_status_v1 string, work_status_v2 string, actual_flag boolean, survey_response_major_dataset_version int",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_work_location_student())
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
