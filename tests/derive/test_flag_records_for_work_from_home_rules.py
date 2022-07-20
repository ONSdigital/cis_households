from chispa import assert_df_equality

from cishouseholds.derive import flag_records_for_work_from_home_rules


def test_flag_records_for_work_from_home_rules(spark_session):
    """Test flag_records_for_work_from_home_rules function correctly flags the records"""

    # below we are flagging "None"'s with 1 everything else 0 - these, Nones/Nulls are
    # what we are expecting the flag_records_for_work_from_home_rules to flag with 1s.
    test_cases = [("office work", False), (None, True), ("construction site", False), (None, True)]

    expected_df = spark_session.createDataFrame(test_cases, schema="work_location string, actual_flag boolean")

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_work_from_home_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
