import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.pipeline.regex_patterns import work_from_home_pattern


# A list of positive test cases which need to be interpreted as "Working from Home" &
# negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": [
        "WORKING FROM HOME",
        "WORKS FROM HOME",
        "WORK FROM HOME",
        "NO CHANGE IN WORK - BOTH STILL WORKING FROM HOME",
        "NO CHANGE IN WORK - CONTINUING TO WORK FROM HOME",
        "WORKING FROM HOME - ENVIRONMENT AGENCY - NO CONTACT WITH COLLEAGUES",
        "WORKING FROM HOME - NO CHANGE IN CIRCUMSTANCES",
        "WORKING FROM HOME FULL TIME - SEE PREVIOUS NOTES",
        "WORKING FROM HOME ON COMPUTER",
        "WORKING FULL TIME FRO HOME",
        "WORKING FULL TIME FROM HOME - SOFTWARE ENGINEER",
        "WORKING FULL TIME FROM HOME IT",
        "WK FROM HOM",
        "WFH",
    ],
    "negative": [
        "WORK WORK WORK",
        "SWEET HOME ALABAMA",
        "FROM DAWN TO DUSK",
        "WORK DAY",
        "WORKING 9 TO 5",
        "HOME WORK",
    ],
}


def test_add_work_from_home_identifier(prepare_regex_test_cases, spark_session):

    wfh_cases = prepare_regex_test_cases(test_data)

    expected_df = spark_session.createDataFrame(wfh_cases, schema="test_case string, is_working_from_home boolean")
    actual_df = assign_regex_match_result(
        df=expected_df.drop("is_working_from_home"),
        columns_to_check_in=["test_case"],
        positive_regex_pattern=work_from_home_pattern.positive_regex_pattern,
        negative_regex_pattern=work_from_home_pattern.negative_regex_pattern,
        column_name_to_assign="is_working_from_home",
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
