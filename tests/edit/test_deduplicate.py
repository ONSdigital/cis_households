from chispa import assert_df_equality

from cishouseholds.edit import dedudiplicate_rows


def test_deduplicate_rows(spark_session):
    expected_df1 = spark_session.createDataFrame(
        data=[("Renault", 1, "VW"), ("Fiat", 2, "Mercedes")],
        schema="""french_cars string, id integer, german_cars string""",
    )
    expected_df2 = spark_session.createDataFrame(
        data=[("Renault", 1, "VW"), ("Fiat", 4, "Porsche"), ("Renault", 3, "Audi"), ("Fiat", 2, "Mercedes")],
        schema="""french_cars string, id integer, german_cars string""",
    )
    input_df = spark_session.createDataFrame(
        data=[
            ("Renault", 1, "VW"),
            ("Fiat", 2, "Mercedes"),
            ("Renault", 3, "Audi"),
            ("Fiat", 4, "Porsche"),
            ("Renault", 3, "Audi"),
        ],
        schema="""french_cars string, id integer, german_cars string""",
    )
    output_df1 = dedudiplicate_rows(input_df, ["french_cars"])
    output_df2 = dedudiplicate_rows(input_df, "all")
    assert_df_equality(output_df1, expected_df1, ignore_row_order=True)
    assert_df_equality(output_df2, expected_df2, ignore_row_order=True)
