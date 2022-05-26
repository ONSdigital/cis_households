from chispa import assert_df_equality

from cishouseholds.pipeline.survey_responses_version_2_ETL import derive_people_in_household_count


def test_derive_people_in_household_count(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            ("A", "1",      None,   None, None, None, None, None, None, None,   'Yes'),
            ("B", "1",      4,      None, None, None, None, None, None, None,   'Yes'),
            ("C", "1",      None,   None, 0,    None, None, None, None, None,   'Yes'),
            ("D", "1",      None,   None, 0,    None, None, None, None, None,   'No'),
            # fmt: on
        ],
        schema="""ons_household_id string, participant_id string,
        household_participants_not_consented_count integer,
        household_participants_not_present_count integer,

        infant_1_age integer, infant_2_age integer,
        person_1_not_consenting_age integer, person_2_not_consenting_age integer, person_1_not_present_age integer, person_2_not_present_age integer,
        household_members_under_2_years string""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            ("A", "1",  0, 0, None, None, None, None, None, None,    1, 0, 1, "1",   'Yes'),
            ("B", "1",  4, 0, None, None, None, None, None, None,    1, 0, 5, "5+",  'Yes'),
            ("C", "1",  0, 0, 0,    None, None, None, None, None,    1, 1, 2, "2",   'Yes'),
            ("D", "1",  0, 0, 0,    None, None, None, None, None,    1, 0, 1, "1",   'No'),
            # fmt: on
        ],
        schema="""ons_household_id string, participant_id string,
            household_participants_not_consented_count integer,
            household_participants_not_present_count integer,

            infant_1_age integer, infant_2_age integer,
            person_1_not_consenting_age integer, person_2_not_consenting_age integer,
            person_1_not_present_age integer, person_2_not_present_age integer,
            household_participant_count integer,
            household_participants_under_2_count integer,
            people_in_household_count integer,
            people_in_household_count_group string,
            household_members_under_2_years string""",
    )
    output_df = derive_people_in_household_count(input_df)
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
