import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.edit import add_at_school_identifier


@pytest.fixture
def at_school_test_cases():
    with open("tests/edit/test_attending_school_coding/attending_school_testcases.yml", "r") as fh:
        test_data = yaml.safe_load(fh)

    test_data_melted = [
        (test_case, pos_or_neg == "positive")
        for pos_or_neg, test_cases in test_data.items()
        for test_case in test_cases
    ]
    return test_data_melted


def test_add_at_school_identifier(at_school_test_cases, spark_session):

    expected_df = spark_session.createDataFrame(at_school_test_cases, schema="test_case string, at_school boolean")
    actual_df = add_at_school_identifier(df=expected_df.drop("at_school"), columns_to_check_in=["test_case"])
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
