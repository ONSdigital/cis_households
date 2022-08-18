import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.pipeline.regex_patterns import healthcare_pattern

# A list of positive test cases which need to be interpreted as Attending School &
# negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": [
        "HOSPITAL WORKER",
        "HOSPITAL CARE ASSISTANT" "JUNIOR MIDWIFE",
        "TRAINEE PHLEBOTOM",
        "DOCTOR",
        "PA TO DOCTOR" "A&E RECEPTIONIST",
        "RECEPTIONIST IN GP CLINIC",
        "DRUG COUNSELLOR",
        "EMERGENCY CALL HANDLER",
        "AMBULANCE DRIVER",
        "OSTEOPATH",
        "MIDWIFE MANAGER",
        "HCA" "DIETICIAN",
        "ASSISTANT DIETICIAN",
        "999 CALL HANDLER",
    ],
    "negative": [
        "SCHOOL NURSE",
        "LECTURING DOCTOR OF PHYSICS",
        "DISCHARGED PATIENT CARER",
        "LOCAL COUNCIL CARER" "SECONDARY SCHOOL TEACHER",
        "PHYSIOSIST",
        "DETECTION",
        "BUSINESS RECEPTIONIST",
    ],
}


def test_healthcare_identifier(prepare_regex_test_cases, spark_session):

    test_cases = prepare_regex_test_cases(test_data)

    expected_df = spark_session.createDataFrame(test_cases, schema="test_case string, work_healthcare boolean")
    actual_df = assign_regex_match_result(
        df=expected_df.drop("work_healthcare"),
        columns_to_check_in=["test_case"],
        positive_regex_pattern=healthcare_pattern.positive_regex_pattern,
        negative_regex_pattern=healthcare_pattern.negative_regex_pattern,
        column_name_to_assign="work_healthcare",
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
