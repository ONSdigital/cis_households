from chispa import assert_df_equality

from cishouseholds.edit import update_person_count_from_ages


def test_update_person_count_from_ages(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (None, 0, 0, 1),
            (1, None, 1, 1),
            (2, None, 0, 0),
            (None, None, None, None),
        ],
        schema="""count integer, 1_age integer, 2_age integer, 3_age integer""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (1, 0, 0, 1),  # Assign count of ages > 0
            (2, None, 1, 1),  # Overwrite with count of ages > 0
            (2, None, 0, 0),  # Keep original when count of ages > 0 is 0
            (0, None, None, None),  # Assign 0
        ],
        schema="""count integer, 1_age integer, 2_age integer, 3_age integer""",
    )

    output_df = update_person_count_from_ages(input_df, "count", r"[1-3]_age")
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
