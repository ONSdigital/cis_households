from chispa import assert_df_equality

from cishouseholds.edit import clean_postcode


def test_clean_postcode(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("AB123CD", 1),
            ("A1  2BC", 2),
            ("AB1 2CD", 3),
            ("AB12 3CD", 4),
            ("A1 2BC", 5),
            ("AB12CD", 6),
            ("12345678", 7),
        ],
        schema="""ref string, id integer""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("AB123CD", 1),
            ("A1  2BC", 2),
            ("AB1 2CD", 3),
            ("AB123CD", 4),
            ("A1  2BC", 5),
            ("AB1 2CD", 6),
            (None, 7),
        ],
        schema="""ref string, id integer""",
    )

    output_df = clean_postcode(input_df, "ref")
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
