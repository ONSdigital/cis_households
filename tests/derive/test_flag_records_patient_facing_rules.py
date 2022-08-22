import pytest
import yaml
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_match_result
from cishouseholds.pipeline.regex_patterns import patient_facing_pattern

# A list of positive test cases which need to be interpreted as Attending School &
# negative test cases which shouldn't.
# Please append new cases to the appropriate list below
test_data = {
    "positive": [
        "JUNIOR MIDWIFE",
        "TRAINEE PHLEBOTOM",
        "DOCTOR",
        "DRUG COUNSELLOR",
        "OSTEOPATH",
        "HCA",
        "COVID SWAB",
        "DENTIST",
        "HOSPITAL CARE ASSISTANT",
        "GP",
        "DIETICIAN",
        "HOSPITAL WORKER",
        "ASSISTANT DIETICIAN",
    ],
    "negative": [
        "SCHOOL NURSE",
        "AMBULANCE DRIVER",
        "RETIRED DOCTOR",
        "GYNECOLOGIST",
        "ORTHODONTIST WORKING FROM HOME",
        "999 CALL HANDLER",
        "MIDWIFE MANAGER",
        "LECTURING DOCTOR OF PHYSICS",
        "DISCHARGED PATIENT CARER",
        "LOCAL COUNCIL CARER",
        "PHYSIOSIST",
        "DETECTION",
        "BUSINESS RECEPTIONIST",
    ],
}


def test_patient_facing_identifier(prepare_regex_test_cases, spark_session):

    test_cases = prepare_regex_test_cases(test_data)

    expected_df = spark_session.createDataFrame(test_cases, schema="test_case string, patient_facing boolean")

    actual_df = assign_regex_match_result(
        df=expected_df.drop("patient_facing"),
        columns_to_check_in=["test_case"],
        positive_regex_pattern=patient_facing_pattern.positive_regex_pattern,
        negative_regex_pattern=patient_facing_pattern.negative_regex_pattern,
        column_name_to_assign="patient_facing",
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
