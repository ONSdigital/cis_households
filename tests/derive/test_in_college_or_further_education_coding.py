import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.pipeline.regex_patterns import in_college_or_further_education_pattern

# A list of positive test cases which need to be interpreted as In College or
# Further Education and negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": [
        "SIXTH FORM",
        "YEAR 12",
        "YEAR 13",
        "APPRENTICESHIPS",
        "COLLEGE",
        "A LEVELS",
        "A-LEVEL",
        "VOCATIONAL",
        "T LEVELS",
        "QUALIFICATION",
    ],
    "negative": [
        "PRIMARY SCHOOL YEAR 6 TEACHER",
        "CATERING ASSISTANT",
        "INTERN",
        "COMMUNITY WORKER",
        "RETIRING THIS YEAR",
    ],
}


def test_add_at_school_identifier(prepare_regex_test_cases, spark_session):

    test_cases = prepare_regex_test_cases(test_data)

    expected_df = spark_session.createDataFrame(
        test_cases, schema="test_case string, in_college_or_further_education boolean"
    )
    actual_df = assign_regex_match_result(
        df=expected_df.drop("in_college_or_further_education"),
        columns_to_check_in=["test_case"],
        positive_regex_pattern=in_college_or_further_education_pattern.positive_regex_pattern,
        negative_regex_pattern=in_college_or_further_education_pattern.negative_regex_pattern,
        column_name_to_assign="in_college_or_further_education",
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
