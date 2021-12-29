from chispa import assert_df_equality

from cishouseholds.derive import assign_ever_long_term_disabled


def test_assign_ever_long_term_disabled(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "Yes", "Yes, a little", "Yes"),
            (1, "Yes", "Yes, a little", "Yes"),
            (1, None, "Not at all", "No"),
            (1, "No", "Not at all", "No"),
        ],
        schema="""
        participant_id integer,
        health string,
        impact string,
        ever string
        """,
    )

    output_df = assign_ever_long_term_disabled(
        expected_df.drop("ever"),
        column_name_to_assign="ever",
        health_conditions_column="health",
        condition_impact_column="impact",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
