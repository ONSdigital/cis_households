import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.regex.regex_patterns import retired_regex_pattern


# A list of positive test cases which need to be interpreted as "Retired" &
# negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": [
        "RETIRED",
        "RETIERD",
        "RETIRDED",
        "RETITED",
        "RETRIED",
        "REFIRED",
        "REITRED",
        "RERTIRED",
        "RETIRED HEAD TEACHER",
    ],
    "negative": [
        "PENSIONS ADVISOR TO SERVING AND RETIRED MILITARY PERSONNEL",
        "HELP TO RETIRED PEOPLE IN THEIR INDEPENDENT LEAVING",
        "SEMI RETIRED",
        "SEMI-RETIRED",
        "PARTIAL RETIRED",
        "SEMIRETIRED",
        "PARTIALLY RETIRED",
    ],
}


def test_add_retired_identifier(prepare_regex_test_cases, spark_session):
    retired_cases = prepare_regex_test_cases(test_data)

    expected_df = spark_session.createDataFrame(retired_cases, schema="test_case string, is_retired boolean")
    actual_df = assign_regex_match_result(
        df=expected_df.drop("is_retired"),
        columns_to_check_in=["test_case"],
        positive_regex_pattern=retired_regex_pattern.positive_regex_pattern,
        negative_regex_pattern=retired_regex_pattern.negative_regex_pattern,
        column_name_to_assign="is_retired",
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
