import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.regex.regex_patterns import at_school_pattern

# A list of positive test cases which need to be interpreted as Attending School &
# negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": [
        "AT SCHOOL",
        "ATTENDING SCHOOL",
        "SCHOOL /CHILD",
        "MINOR",
        "SCHOOL AGE",
        "SCHOOL AGE CHILD",
        "SCHOOL GIRL",
        "SCHOOL PUPIL",
        "CHILD",
        "PRIMARY SCHOOL",
        "PRIMARY SCHOOL YEAR 1",
        "SECONDARY SCHOOL",
        "PRIMARY SCHOOL YEAR 6",
        "SCHOOL AGED CHILD",
        "SCHOOL-AGED CHILD",
        "GOES TO SCHOOL",
        "ATTENDS SCHOOL",
        "IN SCHOOL",
        "EDUCATION IN FINAL YEAR OF SECONDARY SCHOOL",
    ],
    "negative": [
        "TEACHER IN SECONDARY SCHOOL AGED 11 TO 16" "SCHOOL TEACHER",
        "SCHOOL DINNER LADY",
        "PRIMARY CARE GIVER",
        "CHILD MINDER",
        "PRIMARY SCHOOL TEACHER",
        "SECONDARY SCHOOL TEACHER",
        "TEACHER",
        "TEACHING ASSISTANT AT A SCHOOL",
        "CATERING ASSISTANT",
        "SCHOOL BUSINESS MANAGER",
        "SCHOOL HEADMASTER",
        "SCHOOL HEADMISTRESS",
    ],
}


def test_add_at_school_identifier(prepare_regex_test_cases, spark_session):
    at_school_test_cases = prepare_regex_test_cases(test_data)

    expected_df = spark_session.createDataFrame(at_school_test_cases, schema="test_case string, at_school boolean")
    actual_df = assign_regex_match_result(
        df=expected_df.drop("at_school"),
        columns_to_check_in=["test_case"],
        positive_regex_pattern=at_school_pattern.positive_regex_pattern,
        negative_regex_pattern=at_school_pattern.negative_regex_pattern,
        column_name_to_assign="at_school",
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
