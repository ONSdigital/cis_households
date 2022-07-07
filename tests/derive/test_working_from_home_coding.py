import pytest
import yaml
from chispa import assert_df_equality
from working_from_home_testcases import test_data

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.regex_patterns import work_from_home_pattern


@pytest.fixture
def wfh_cases():
    test_data_melted = [
        (test_case, pos_or_neg == "positive")
        for pos_or_neg, test_cases in test_data.items()
        for test_case in test_cases
    ]
    return test_data_melted


def test_add_work_from_home_identifier(wfh_cases, spark_session):

    expected_df = spark_session.createDataFrame(wfh_cases, schema="test_case string, is_working_from_home boolean")
    actual_df = assign_regex_match_result(
        df=expected_df.drop("is_working_from_home"),
        column_name_to_assign="is_working_from_home",
        columns_to_check_in=["test_case"],
        positive_regex_pattern=work_from_home_pattern.positive_regex_pattern,
        negative_regex_pattern=work_from_home_pattern.negative_regex_pattern,
        return_column_object=False,
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
