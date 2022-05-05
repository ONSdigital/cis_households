from chispa import assert_df_equality

from cishouseholds.derive import derive_had_self_isolating_from_digital


def test_derive_had_self_isolating_from_digital(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (
                "I have or have had symptoms of COVID-19 or a positive test",
                "Yes because you have/have had symptoms of COVID-19 or a positive test",
                "Yes",
            ),
            (
                "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",
                "Yes because you live with someone who has/has had symptoms or a positive test but you haven’t had symptoms yourself",
                "Yes",
            ),
            (
                "Due to increased risk of getting COVID-19 such as having been in contact with a known case or quarantining after travel abroad",
                "Yes for other reasons related to you having had an increased risk of getting COVID-19 (e.g. having been in contact with a known case or quarantining after travel abroad)",
                "Yes",
            ),
            (
                "Due to reducing my risk of getting COVID-19 such as going into hospital or shielding",
                "Yes for other reasons related to reducing your risk of getting COVID-19 (e.g. going into hospital or shielding)",
                "Yes",
            ),
            ("Due to reducing my risk of getting COVID-19 such as going into hospital or shielding", "No", "No"),
        ],
        schema="""
        self_isolating_reason string,
        self_isolating_transformed string,
        self_isolating string
        """,
    )

    output_df = derive_had_self_isolating_from_digital(
        expected_df.drop("self_isolating_transformed"),
        "self_isolating_transformed",
        "self_isolating",
        "self_isolating_reason",
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
