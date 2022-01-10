from chispa import assert_df_equality

from cishouseholds.derive import assign_ever_had_long_term_health_condition_or_disabled


def test_assign_ever_had_long_term_health_condition_or_disabled(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "Yes", "Yes, a little", "Yes"),
            (1, "Yes", "Yes, a little", "Yes"),
            (2, None, "Not at all", "No"),
            (2, "No", "Not at all", "No"),
        ],
        schema="""
        participant_id integer,
        health string,
        impact string,
        ever string
        """,
    )

    output_df = assign_ever_had_long_term_health_condition_or_disabled(
        expected_df.drop("ever"),
        column_name_to_assign="ever",
        health_conditions_column="health",
        condition_impact_column="impact",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
