import pytest
import yaml
from at_university_testcases import test_data
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.regex_patterns import at_university_pattern


@pytest.fixture
def at_university_test_cases():
    test_data_melted = [
        (test_case, pos_or_neg == "positive")
        for pos_or_neg, test_cases in test_data.items()
        for test_case in test_cases
    ]
    return test_data_melted


def test_add_at_university_identifier(at_university_test_cases, spark_session):

    expected_df = spark_session.createDataFrame(
        at_university_test_cases, schema="test_case string, at_university boolean"
    )
    actual_df = assign_regex_match_result(
        df=expected_df.drop("at_university"),
        column_name_to_assign="at_university",
        columns_to_check_in=["test_case"],
        positive_regex_pattern=at_university_pattern.positive_regex_pattern,
        negative_regex_pattern=at_university_pattern.negative_regex_pattern,
        return_column_object=False,
    )

    a = actual_df.collect()

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
