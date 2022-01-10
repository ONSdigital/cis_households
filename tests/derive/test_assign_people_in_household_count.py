from chispa import assert_df_equality

from cishouseholds.derive import assign_people_in_household_count


def test_assign_people_in_household_count(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 2, 3, 4, None, 4),
            (1, 2, 3, 4, 6, 5),
        ],
        schema="participant_count integer, infant_1_age integer, infant_2_age integer, person_1_not_consenting_age integer, person_1_age integer, total integer",
    )
    output_df = assign_people_in_household_count(expected_df.drop("total"), "total", "participant_count")
    assert_df_equality(output_df, expected_df)
