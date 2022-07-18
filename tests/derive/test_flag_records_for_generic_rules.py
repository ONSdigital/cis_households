import pytest
from chispa import assert_df_equality

from cishouseholds.derive import flag_records_for_generic_rules


def test_flag_records_for_generic_rules(spark_session):
    """Test flag_records_for_generic_rules function correctly flags the records
    indicated by categories_list"""

    # below we are flagging "a" & "c" with 1 everything else 0
    test_cases = [
        ("a", True),
        ("b", False),
        ("c", True),
        ("d", False),
        ("c", True),
    ]

    expected_df = spark_session.createDataFrame(test_cases, schema="my_column string, actual_flag boolean")

    actual_df = expected_df.drop("actual_flag").withColumn(
        "actual_flag", flag_records_for_generic_rules(column_to_check_in="my_column", categories_list=["a", "c"])
    )

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
