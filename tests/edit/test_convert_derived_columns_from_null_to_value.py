import pytest
from chispa import assert_df_equality

from cishouseholds.edit import convert_derived_columns_from_null_to_value


@pytest.fixture
def infection_sympton_dict():
    return {
        "phm_think_had_flu": ("survey_completion_status", "Completed"),
        "phm_think_had_respiratory_infection": ("survey_completion_status", "Completed"),
        "phm_think_had_covid": ("survey_completion_status", "Completed"),
        "phm_think_had_unknown": ("survey_completion_status", "Completed"),
        "think_have_covid_symptom": ("survey_completion_status", "Completed"),
        "think_had_covid_symptom": ("phm_think_had_covid", "Yes"),
        "think_had_other_infection_symptom": ("phm_think_had_other_infection", "Yes"),
        "think_have_long_covid_symptom": ("think_have_long_covid", "Yes"),
        "think_had_flu_symptom": ("phm_think_had_flu", "Yes"),
    }


def test_symtom_columns_converted_to_no_when_infection_column_states_infection(spark_session, infection_sympton_dict):
    schema = """phm_think_had_covid string, think_had_covid_symptom_runny_nose string, think_had_covid_symptom_sneezing string, think_had_covid_symptom_coughing string, think_had_covid_symptom_headache string, think_had_covid_symptom_fever string,
    phm_think_had_flu string, think_had_flu_symptom_runny_nose string, think_had_flu_symptom_sneezing string, think_had_flu_symptom_coughing string, think_had_flu_symptom_headache string, think_had_flu_symptom_fever string,
    survey_completion_status string, phm_think_had_respiratory_infection string, think_have_covid_symptom_cough string, think_have_covid_symptom_sore_throat string"""
    input_data = [
        (
            "Yes",
            "Yes",
            "Yes",
            None,
            None,
            "Yes",
            "Yes",
            "Yes",
            "Yes",
            None,
            None,
            "Yes",
            "Completed",
            None,
            None,
            "Yes",
        ),
        ("No", None, None, None, None, None, "No", None, None, None, None, None, "Non-response", None, "Yes", None),
        ("Yes", "Yes", None, None, None, "Yes", "No", None, None, None, None, None, "Completed", "Yes", None, None),
        ("Yes", None, "Yes", None, "Yes", "Yes", "Yes", "Yes", "Yes", None, None, "Yes", "Completed", None, None, None),
    ]

    input_df = spark_session.createDataFrame(data=input_data, schema=schema)

    expected_data = [
        (
            "Yes",
            "Yes",
            "Yes",
            "No",
            "No",
            "Yes",
            "Yes",
            "Yes",
            "Yes",
            "No",
            "No",
            "Yes",
            "Completed",
            "No",
            "No",
            "Yes",
        ),
        ("No", None, None, None, None, None, "No", None, None, None, None, None, "Non-response", None, "Yes", None),
        ("Yes", "Yes", "No", "No", "No", "Yes", "No", None, None, None, None, None, "Completed", "Yes", "No", "No"),
        ("Yes", "No", "Yes", "No", "Yes", "Yes", "Yes", "Yes", "Yes", "No", "No", "Yes", "Completed", "No", "No", "No"),
    ]

    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)

    actual_df = convert_derived_columns_from_null_to_value(input_df, infection_sympton_dict, "No")

    assert_df_equality(actual_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
