from chispa import assert_df_equality

from cishouseholds.edit import convert_symptom_columns_to_no_if_null


def test_symtom_columns_converted_to_no_when_infection_column_states_infection(spark_session):
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

    actual_df = convert_symptom_columns_to_no_if_null(input_df)

    assert_df_equality(actual_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
