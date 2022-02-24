from chispa import assert_df_equality

from cishouseholds.edit import update_from_csv_lookup


def test_update_from_csv_lookup(spark_session, pandas_df_to_temporary_csv):
    map_df = spark_session.createDataFrame(
        data=[
            (1, "B", "original", "converted"),
            (2, "A", "original", "converted"),
            (2, "B", "original", "converted"),
            (3, "B", "original", "converted"),
            (4, "B", "original", "converted"),
            (5, "A", "something else", "converted"),
            (6, "A", "value", "converted A from null"),
            (6, "B", None, "converted B from null"),
        ],
        schema="""id integer, target_column_name string, old_value string, new_value string""",
    )
    csv_filepath = pandas_df_to_temporary_csv(map_df.toPandas()).as_posix()
    input_df = spark_session.createDataFrame(
        data=[
            (1, "original", "original"),
            (2, "original", "original"),
            (3, "original", "original"),
            (4, "original", "original"),
            (5, "original", "original"),
            (6, "value", None),
        ],
        schema="""id integer, A string, B string""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "original", "converted"),
            (2, "converted", "converted"),
            (3, "original", "converted"),
            (4, "original", "converted"),
            (5, "original", "original"),
            (6, "converted A from null", "converted B from null"),
        ],
        schema="""id integer, A string, B string""",
    )
    df = input_df
    map = csv_filepath

    output_df = update_from_csv_lookup(df, map, "id")
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True)
