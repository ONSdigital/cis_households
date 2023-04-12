from chispa import assert_df_equality

from cishouseholds.pipeline.version_specific_processing.phm_transformations import assign_any_symptoms


# def test_assign_any_symptoms(spark_session):
#     expected_df = spark_session.createDataFrame(
#         data=[
#             (
#                 1,
#                 "Yes",
#                 "None of these symptoms",
#                 "None of these symptoms",
#                 "None of these symptoms",
#                 None,
#                 None,
#                 None,
#                 "No",
#             ),
#             (2, "Yes", None, "None of these symptoms", "None of these symptoms", "symptom a", None, None, "Yes"),
#             (3, None, None, None, None, None, None, None, None),
#             (4, "Yes", None, None, 0, "symptom a", "symptom a", "symptom a", "Yes"),
#         ],
#         schema="""
#         id integer,
#         think_have_long_covid string,
#         think_have_long_covid_no_symptoms_list_1 string,
#         think_have_long_covid_no_symptoms_list_2 string,
#         think_have_long_covid_no_symptoms_list_3 string,
#         think_have_long_covid_any_symptom_list_1 string,
#         think_have_long_covid_any_symptom_list_2 string,
#         think_have_long_covid_any_symptom_list_3 string,
#         think_have_covid_no_symptoms_list_1 string,
#         think_have_covid_no_symptoms_list_2 string,
#         think_have_covid_any_symptom_list_1 string,
#         think_have_covid_any_symptom_list_2 string,
#         think_have_no_symptoms_new_or_worse_list_1 string,
#         think_have_no_symptoms_new_or_worse_list_2 string,
#         think_have_any_symptoms_new_or_worse_list_1 string,
#         think_have_any_symptoms_new_or_worse_list_2 string,
#         phm_think_had_covid string,
#         think_had_covid_no_symptoms_list_1 string,
#         think_had_covid_no_symptoms_list_2 string,
#         think_had_covid_any_symptom_list_1 string,
#         think_had_covid_any_symptom_list_2 string,
#         phm_think_had_flu string,
#         think_had_flu_no_symptoms_list_1 string,
#         think_had_flu_no_symptoms_list_2 string,
#         think_had_flu_any_symptom_list_1 string,
#         think_had_flu_any_symptom_list_2 string,
#         phm_think_had_other_infection string,
#         think_had_other_infection_no_symptoms_list_1 string,
#         think_had_other_infection_no_symptoms_list_2 string,
#         think_had_other_infection_any_symptom_list_1 string,
#         think_had_other_infection_any_symptom_list_2 string,
#         think_have_long_covid_any_symptoms string,
#         think_have_covid_any_symptoms string,
#         think_have_any_symptoms_new_or_worse string,
#         think_have_long_covid_any_symptoms string,
#         think_had_covid_any_symptoms string,
#         think_had_flu_any_symptoms string,
#         think_had_other_infection_any_symptoms string,
#         """,
#     )
#     input_df = expected_df.drop("think_have_long_covid_any_symptoms")

#     output_df = assign_any_symptoms(input_df)
#     assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
