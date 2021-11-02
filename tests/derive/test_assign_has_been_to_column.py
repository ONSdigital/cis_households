from chispa import assert_df_equality

from cishouseholds.derive import assign_has_been_to_column


def test_assign_has_been_to_column(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("Yes, I have", "Yes", "No"),
            ("No, no one in my household has", "No", "No"),
            ("No I haven't, but someone else in my household has", "No", "Yes"),
            ("Yes, I have", "Yes", "Yes"),
        ],
        schema="""
        has_been_to string,
        participant string,
        other string
        """,
    )

    input_df = expected_df.drop("has_been_to")

    output_df = assign_has_been_to_column(input_df, "has_been_to", "participant", "other")

    assert_df_equality(output_df, expected_df, ignore_column_order=True)
