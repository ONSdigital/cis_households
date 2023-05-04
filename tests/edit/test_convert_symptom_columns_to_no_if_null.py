import pytest
from chispa import assert_df_equality

from cishouseholds.edit import convert_derived_columns_from_null_to_no


@pytest.fixture
def infection_sympton_dict():
    return {
        "phm_think_had_covid": "think_had_covid_symptom",
        "phm_think_had_other_infection": "think_had_other_infection_symptom",
        "think_have_long_covid": "think_have_long_covid_symptom",
        "phm_think_had_flu": "think_had_flu_symptom",
    }


def test_symtom_columns_converted_to_no_when_infection_column_states_infection(spark_session, infection_sympton_dict):
    schema = """phm_think_had_covid string, think_had_covid_symptom_runny_nose string, think_had_covid_symptom_sneezing string, think_had_covid_symptom_coughing string, think_had_covid_symptom_headache string, think_had_covid_symptom_fever string,
    phm_think_had_flu string, think_had_flu_symptom_runny_nose string, think_had_flu_symptom_sneezing string, think_had_flu_symptom_coughing string, think_had_flu_symptom_headache string, think_had_flu_symptom_fever string"""
    input_data = [
        ("Yes", "Yes", "Yes", None, None, "Yes", "Yes", "Yes", "Yes", None, None, "Yes"),
        ("No", None, None, None, None, None, "No", None, None, None, None, None),
        ("Yes", "Yes", None, None, None, "Yes", "No", None, None, None, None, None),
        ("Yes", None, "Yes", None, "Yes", "Yes", "Yes", "Yes", "Yes", None, None, "Yes"),
    ]

    input_df = spark_session.createDataFrame(data=input_data, schema=schema)

    expected_data = [
        ("Yes", "Yes", "Yes", "No", "No", "Yes", "Yes", "Yes", "Yes", "No", "No", "Yes"),
        ("No", None, None, None, None, None, "No", None, None, None, None, None),
        ("Yes", "Yes", "No", "No", "No", "Yes", "No", None, None, None, None, None),
        ("Yes", "No", "Yes", "No", "Yes", "Yes", "Yes", "Yes", "Yes", "No", "No", "Yes"),
    ]

    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)

    actual_df = convert_derived_columns_from_null_to_no(input_df, infection_sympton_dict)

    assert_df_equality(actual_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
