from chispa import assert_df_equality

from cishouseholds.edit import format_string_upper_and_clean


def test_format_string_upper_and_clean(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("       this is a   string          .   ", 1),
        ],
        schema="""ref string, id integer""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("THIS IS A STRING", 1),
        ],
        schema="""ref string, id integer""",
    )

    output_df = format_string_upper_and_clean(input_df, "ref")
    assert_df_equality(expected_df, output_df)
