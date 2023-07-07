from chispa import assert_df_equality

from cishouseholds.regex.regex_flags import flag_records_for_work_from_home_rules


def test_flag_records_for_work_from_home_rules(spark_session):
    """Test flag_records_for_work_from_home_rules function correctly flags the records"""

    test_cases = [
        (None, 16, "Student", False),
        (None, 15, "Student", False),
        (None, 19, "Furloughed (temporarily not working)", False),
        (None, 24, "Employed", True),
        ("Client site", 35, "Self-Employed", False),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases, schema="work_location string, age_at_visit int, work_status_v0 string, actual_flag boolean"
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_work_from_home_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
