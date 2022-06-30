import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.edit import add_work_from_home_identifier


@pytest.fixture
def wfh_cases():
    with open("tests/edit/test_wfh_coding/wfh-cases.yml", "r") as fh:
        test_data = yaml.safe_load(fh)

    test_data_melted = [
        (test_case, int(pos_or_neg == "positive"))
        for pos_or_neg, test_cases in test_data.items()
        for test_case in test_cases
    ]
    return test_data_melted


def test_add_work_from_home_identifier(wfh_cases, spark_session):

    expected_df = spark_session.createDataFrame(wfh_cases, schema="test_case string, is_wfh integer")
    actual_df = add_work_from_home_identifier(
        df=expected_df.drop("is_wfh"),
        columns_to_check_in=["test_case"],
        regex_pattern="(W(K|ORK.*?) F(ROM?) H(OME?))|(WFH)",
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
