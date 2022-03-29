from chispa import assert_df_equality

from cishouseholds.edit import update_participant_not_consented


def test_update_participant_not_consented(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, 0, 0, 1),
            (1, None, 0, 0),
            (None, 0, 1, 1),
        ],
        schema="""count string, p1_age integer, p2_age integer, p3_age integer""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (1, 0, 0, 1),
            (1, None, 0, 0),
            (2, 0, 1, 1),
        ],
        schema="""count integer, p1_age integer, p2_age integer, p3_age integer""",
    )

    output_df = update_participant_not_consented(input_df, "count", r"p\d{1,}_age")
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
