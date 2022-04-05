from chispa import assert_df_equality

from cishouseholds.pipeline.survey_responses_version_2_ETL import derive_people_in_household_count


def test_derive_people_in_household_count(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("A", "1", None, None, None, None, None, None, None, None),
            ("B", "1", 4, None, None, None, None, None, None, None),
            ("C", "1", None, None, 0, None, None, None, None, None),
        ],
        schema="""ons_household_id string, participant_id string, household_participants_not_consented_count integer, household_participants_not_present_count integer, infant_1_age integer, infant_2_age integer, person_1_not_consenting_age integer, person_2_not_consenting_age integer, person_1_not_present_age integer, person_2_not_present_age integer""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "1", 0, 0, None, None, None, None, None, None, 1, 0, 1),
            ("B", "1", 4, 0, None, None, None, None, None, None, 1, 0, 5),
            ("C", "1", 0, 0, 0, None, None, None, None, None, 1, 1, 2),
        ],
        schema="""ons_household_id string, participant_id string, household_participants_not_consented_count integer, household_participants_not_present_count integer, infant_1_age integer, infant_2_age integer, person_1_not_consenting_age integer, person_2_not_consenting_age integer, person_1_not_present_age integer, person_2_not_present_age integer, household_participant_count integer, household_participants_under_2_count integer, people_in_household_count integer""",
    )

    output_df = derive_people_in_household_count(input_df)
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
