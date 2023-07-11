import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.regex.regex_patterns import furloughed_pattern

# A list of positive test cases which need to be interpreted as Furloughed &
# negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": [
        "CURRENTLY FURLOUGHED",
        "FULRLOUGHED",
        "FURLOGHED (TEST TUBE PRODUCTION FOR NHS)",
        "FURLOGED",
        "FURLOUGH",
        "FURLOUGHED",
        "FURLOUGHED AT THE MOMENT",
        "FURLOUGHED/SELF-EMPLOYMENT",
        "FURLOWED",
        "FURLOWEDFURLOUGHED",
        "TEACHER FURLOUGHED",
    ],
    "negative": [
        "NOT ON FURLOUGH",
        "FURLOUGHED ON AND OFF CURRENTLY WORKING",
    ],
}


def test_furloughed_identifier(prepare_regex_test_cases, spark_session):
    test_cases = prepare_regex_test_cases(test_data)

    expected_df = spark_session.createDataFrame(test_cases, schema="test_case string, furloughed boolean")
    actual_df = assign_regex_match_result(
        df=expected_df.drop("furloughed"),
        columns_to_check_in=["test_case"],
        positive_regex_pattern=furloughed_pattern.positive_regex_pattern,
        negative_regex_pattern=furloughed_pattern.negative_regex_pattern,
        column_name_to_assign="furloughed",
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
