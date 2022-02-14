from chispa import assert_df_equality

from cishouseholds.edit import map_column_values_to_null


def test_map_column_values_to_null(spark_session):
    input_df = spark_session.createDataFrame(
        data=[("Renault", 1), ("Fiat", 2), ("Kia", 3), ("Fiat", 4)],
        schema="""car string, id integer""",
    )
    expected_df = spark_session.createDataFrame(
        data=[("Renault", 1), (None, 2), ("Kia", 3), (None, 4)],
        schema="""car string, id integer""",
    )
    output_df = map_column_values_to_null(input_df, ["car"], "Fiat")
    assert_df_equality(output_df, expected_df)
