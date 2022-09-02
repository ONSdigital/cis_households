import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.pipeline.regex_patterns import at_university_pattern


# A list of positive test cases which need to be interpreted as Attending University &
# negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": [
        "AT COLLEGE",
        "FULL TIME EDUCATION",
        "FULL TIME STUDENT",
        "N/A (STUDENT)",
        "PHD STUDENT",
        "STIDY",
        "STUDENT",
        "STUDENT FULL TIME",
        "STUDENT ONLY",
        "STUDENT UNIVERSITY",
        "STUDT",
        "STUDWNY",
        "STUDY",
        "STUDYING",
        "STUDYING AT UNIVERSITY",
        "STUDYING MECHANICAL ENGINEERING",
        "UNIVERSITY STUDENT",
        "UNI",
    ],
    "negative": [
        "LEARNING OFFICER",
        "INTERN",
        "UNIVERSITY PROFESSOR",
        "ASSISTANT PROFESSOR",
        "LECTURER AT UNIVERSITY",
        "COMMUNITY WORKER",
    ],
}


def test_add_at_university_identifier(prepare_regex_test_cases, spark_session):

    at_university_test_cases = prepare_regex_test_cases(test_data)

    expected_df = spark_session.createDataFrame(
        at_university_test_cases, schema="test_case string, at_university boolean"
    )
    actual_df = assign_regex_match_result(
        df=expected_df.drop("at_university"),
        columns_to_check_in=["test_case"],
        positive_regex_pattern=at_university_pattern.positive_regex_pattern,
        negative_regex_pattern=at_university_pattern.negative_regex_pattern,
        column_name_to_assign="at_university",
    )

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
