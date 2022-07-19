import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.pipeline.regex_patterns import self_employed_regex


# A list of positive test cases which need to be interpreted as "Self Employed" &
# negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": ["SELFEMPLOYED", "SELF-EMPLOYED", "SELF EMPLOYED"],
    "negative": [
        "SELFRIDGES",
        "UNEMPLOYED",
        "SELF UNEMPLOYED",
        "WORKS FOR WIFE",
        "WORKING 9 TO 5",
        "HOME WORK",
    ],
}


def test_add_self_employed_identifier(prepare_regex_test_cases, spark_session):

    test_cases = prepare_regex_test_cases(test_data)

    expected_df = spark_session.createDataFrame(test_cases, schema="test_case string, is_self_employed boolean")
    actual_df = assign_regex_match_result(
        df=expected_df.drop("is_self_employed"),
        columns_to_check_in=["test_case"],
        positive_regex_pattern=self_employed_regex.positive_regex_pattern,
        column_name_to_assign="is_self_employed",
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
