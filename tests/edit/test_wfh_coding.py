import yaml
from chispa import assert_df_equality

from cishouseholds.edit import add_work_from_home_identifier
from cishouseholds.weights.design_weights import household_level_populations


def test_add_work_from_home_identifier(spark_session):

    with open("./test_wfh_coding/wfh-cases.yml", "r") as fh:
        test_data = yaml.safe_load(fh)

    test_data_melted = [
        (test_case, pos_or_neg == "positive")
        for pos_or_neg, test_cases in test_data.items()
        for test_case in test_cases
    ]

    expected_df = spark_session.createDataFrame(test_data_melted, schema="is_wfh bool, test_case string")
    actual_df = add_work_from_home_identifier(expected_df.drop("is_wfh"), "(W(K|ORK.*?) F(ROM?) H(OME?))|(WFH)")

    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
