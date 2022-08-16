import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.pipeline.regex_patterns import childcare_pattern

# A list of positive test cases which need to be interpreted as Attending School &
# negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": [
        "NURSREY",
        "NURSERY",
        "NUSERY",
        "NURSARY",
        "NURSEY",
        "CRECHE",
        "DAY CARE",
        "DAYCARE",
        "CHILDMINDER",
        "PLAYGROUP",
        "PLAY GROUP",
        "STUDENT",
        "EDUCATION",
        "PRE SCHOOL",
        "LEARNER",
        "CHILDCARE",
    ],
    "negative": [
        "NURSE" "REGISTERED NURSE",
        "NURSING HOME SUPERVISOR",
    ],
}


def test_childcare_identifier(prepare_regex_test_cases, spark_session):

    test_cases = prepare_regex_test_cases(test_data)

    expected_df = spark_session.createDataFrame(test_cases, schema="test_case string, in_childcare boolean")

    actual_df = assign_regex_match_result(
        df=expected_df.drop("in_childcare"),
        columns_to_check_in=["test_case"],
        positive_regex_pattern=childcare_pattern.positive_regex_pattern,
        negative_regex_pattern=childcare_pattern.negative_regex_pattern,
        column_name_to_assign="in_childcare",
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
