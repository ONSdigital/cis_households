from chispa import assert_df_equality

from cishouseholds.derive import flag_records_for_retired_rules_v0
from cishouseholds.derive import flag_records_for_retired_rules_v1
from cishouseholds.derive import flag_records_for_retired_rules_v2


def test_flag_records_for_retired_rules_v0(spark_session):
    """Test flag_records_for_retired_rules function correctly flags the records"""

    test_cases = [
        (0, "a", "b", "c", "d", "e", 10, False),
        (1, "a", "b", "c", "d", "e", 100, False),
        (0, None, None, None, "d", None, 100, False),
        (0, None, None, "f", None, None, 100, True),
        (2, None, None, None, None, None, 100, False),
        (0, None, None, "c", None, None, 76, True),
        (1, None, None, "c", None, None, 75, False),
        (0, None, None, "c", None, None, 74, False),
    ]

    expected_df = spark_session.createDataFrame(
        [(idx, *i) for idx, i in enumerate(test_cases)],
        schema=(
            "row_id int, survey_response_dataset_major_version int, work_status_v0 string, work_status_v1 string, work_status_v2 string, work_main_job_role string, work_main_job_title string, age_at_visit int, actual_flag boolean"
        ),
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_retired_rules_v0())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=False,
        ignore_nullable=True,
    )


def test_flag_records_for_retired_rules_v1(spark_session):
    """Test flag_records_for_retired_rules function correctly flags the records"""

    test_cases = [
        (1, "a", "b", "c", "d", "e", 10, False),
        (1, "a", "b", "c", "d", "e", 100, False),
        (1, "a", None, None, "d", None, 100, False),
        (1, "a", None, None, None, None, 100, True),
        (2, None, None, None, None, None, 100, False),
        (3, None, None, None, None, None, 76, False),
        (0, None, None, None, None, None, 75, False),
        (0, None, None, "c", None, None, 74, False),
    ]

    expected_df = spark_session.createDataFrame(
        [(idx, *i) for idx, i in enumerate(test_cases)],
        schema=(
            "row_id int, survey_response_dataset_major_version int, work_status_v0 string, work_status_v1 string, work_status_v2 string, work_main_job_role string, work_main_job_title string, age_at_visit int, actual_flag boolean"
        ),
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_retired_rules_v1())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=False,
        ignore_nullable=True,
    )


def test_flag_records_for_retired_rules_v2(spark_session):
    """Test flag_records_for_retired_rules function correctly flags the records"""

    test_cases = [
        (1, "a", "b", "c", "d", "e", 10, False),
        (1, "a", "b", "c", "d", "e", 100, False),
        (2, "a", None, None, "d", None, 100, False),
        (0, "a", None, None, None, None, 100, False),
        (2, None, None, None, None, None, 100, True),
        (1, None, None, "c", None, None, 76, False),
        (2, None, "c", None, None, None, 80, True),
        (2, None, "c", None, None, None, 74, False),
    ]

    expected_df = spark_session.createDataFrame(
        [(idx, *i) for idx, i in enumerate(test_cases)],
        schema=(
            "row_id int, survey_response_dataset_major_version int, work_status_v0 string, work_status_v1 string, work_status_v2 string, work_main_job_role string, work_main_job_title string, age_at_visit int, actual_flag boolean"
        ),
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_retired_rules_v2())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=False,
        ignore_nullable=True,
    )
