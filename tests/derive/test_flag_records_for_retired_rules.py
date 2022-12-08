from chispa import assert_df_equality

from cishouseholds.derive import flag_records_for_retired_rules_v0
from cishouseholds.derive import flag_records_for_retired_rules_v1
from cishouseholds.derive import flag_records_for_retired_rules_v2


def test_flag_records_for_retired_rules(spark_session):
    """Test flag_records_for_retired_rules function correctly flags the records"""

    test_cases = [
        ("a", "b", "c", "d", "e", 10, False),
        ("a", "b", "c", "d", "e", 100, False),
        ("a", None, None, "d", None, 100, False),
        ("a", None, None, None, None, 100, True),
        (None, None, None, None, None, 100, True),
        (None, None, "c", None, None, 76, True),
        (None, None, "c", None, None, 75, False),
        (None, None, "c", None, None, 74, False),
    ]

    expected_df = spark_session.createDataFrame(
        [(idx, *i) for idx, i in enumerate(test_cases)],
        schema=(
            "row_id int, work_status_v0 string, work_status_v1 string, work_status_v2 string, work_main_job_role string, work_main_job_title string, age_at_visit int, actual_flag boolean"
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
