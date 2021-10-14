from chispa import assert_df_equality

from cishouseholds.edit import convert_null_if_not_in_list


def test_convert_null_if_not_in_list(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("Renault", 1), ("Fiat", 2), (None, 3), (None, 4)],
        schema="""french_cars string, id integer""",
    )
    input_df = spark_session.createDataFrame(
        data=[("Renault", 1), ("Fiat", 2), ("Kia", 3), ("Honda", 4)],
        schema="""french_cars string, id integer""",
    )
    output_df = convert_null_if_not_in_list(input_df, "french_cars", ["Renault", "Fiat"])
    assert_df_equality(output_df, expected_df)
