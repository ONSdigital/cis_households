import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.regex.regex_patterns import not_working_pattern


# A list of positive test cases which need to be interpreted as "Not working" &
# negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": [
        "NOT CURRENTLY WORKING",
        "NO WORKING",
        "BUT NOT CURRENTLY WORKING",
        "UNEMPLOYED",
        "IM UNEMPLOYED",
        "UNEMPLOYMENT",
        "UNABLE",
        "NIL",
        "MOTHERS CARER",
        "SONS CARER",
        "SON'S CARER",
        "SISTERS CARER",
        "DAUGHTERS CARER",
        "PATERNITY LEAVE",
        "ON PATERNITY LEAVE!",
        "MATERNITY LEAVE",
        "HOUSEWIFE",
        "HOUSE HUSBAND" "HOUSE WIFE",
        "HOMEMAKER",
        "HOME WIFE",
        "FULL TIME MOM",
        "FULL TIME GRANDAD",
        "FULLTIME MOM",
        "AT HOME",
    ],
    "negative": [
        "NOTARY",
        "SONS FOOTBALL COACH",
        "HOME RENOVATOR",
        "HOME DECORATOR",
        "HOUSE HUNTER",
        "HOUSE MASTER",
        "PATERNITY LEAVE COVER",
        "GRAPHIC DESIGN...HOMES SECTOR",
        "DESIGNS HOMES",
        "CARE HOMES WORKER",
    ],
}


def test_add_not_working_identifier(prepare_regex_test_cases, spark_session):
    test_cases = prepare_regex_test_cases(test_data)

    expected_df = spark_session.createDataFrame(test_cases, schema="test_case string, is_not_working boolean")
    actual_df = assign_regex_match_result(
        df=expected_df.drop("is_not_working"),
        columns_to_check_in=["test_case"],
        positive_regex_pattern=not_working_pattern.positive_regex_pattern,
        negative_regex_pattern=not_working_pattern.negative_regex_pattern,
        column_name_to_assign="is_not_working",
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
