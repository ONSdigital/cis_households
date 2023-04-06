from chispa import assert_df_equality

from cishouseholds.pipeline.phm_transformation import assign_any_symptoms


def test_assign_any_symptoms(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (
                1,
                "Yes",
                "None of these symptoms",
                "None of these symptoms",
                "None of these symptoms",
                None,
                None,
                None,
                "No",
            ),
            (2, "Yes", None, "None of these symptoms", "None of these symptoms", "symptom a", None, None, "Yes"),
            (3, None, None, None, None, None, None, None, None),
            (4, "Yes", None, None, 0, "symptom a", "symptom a", "symptom a", "Yes"),
        ],
        schema="""
        id int,
        think_have_long_covid string,
        think_have_long_covid_no_symptoms_list_1 string,
        think_have_long_covid_no_symptoms_list_2 string,
        think_have_long_covid_no_symptoms_list_3 string,
        think_have_long_covid_any_symptom_list_1 string,
        think_have_long_covid_any_symptom_list_2 string,
        think_have_long_covid_any_symptom_list_3 string,
        think_have_long_covid_any_symptoms,
        """,
    )
    input_df = expected_df.drop("think_have_long_covid_any_symptoms")

    output_df = assign_any_symptoms(input_df)
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
