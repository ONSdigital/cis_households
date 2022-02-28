from chispa import assert_df_equality

from cishouseholds.derive import assign_household_participant_count


def test_assign_household_participant_count(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 1, 1),
            (2, 1, 2),
            (2, 2, 2),
        ],
        schema="household integer, participant integer, participants_in_household integer",
    )
    input_df = expected_df.drop("participants_in_household")

    output_df = assign_household_participant_count(input_df, "participants_in_household", "household", "participant")
    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
