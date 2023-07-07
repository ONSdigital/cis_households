from chispa import assert_df_equality

from cishouseholds.regex.regex_flags import flag_records_for_work_location_null
from cishouseholds.regex.regex_flags import flag_records_for_work_location_student


def test_flag_records_for_work_location_null(spark_session):
    """Test flag_records_for_work_location_null function correctly flags the records"""

    test_cases = [
        (None, "Student", "Student", "Go to School", True),
        ("Office", "Employed", None, None, False),
        ("Office", "Furloughed (temporarily not working)", None, None, True),
        ("Office", "Not working (unemployed, retired, long-term sick etc.)", None, None, True),
        ("Office", "Student", None, None, True),
        ("Office", "Self-Employed", None, None, False),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="work_location string, work_status_v0 string, work_main_job_title string, work_main_job_role string, actual_flag boolean",
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
        (16, "Employed", False),
        (15, "Student", True),
        (19, "Furloughed (temporarily not working)", False),
        (24, "Employed", False),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases, schema="age_at_visit int, work_status_v0 string, actual_flag boolean"
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_work_location_student())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
