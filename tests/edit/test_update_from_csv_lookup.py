from chispa import assert_df_equality

from cishouseholds.edit import update_from_csv_lookup


def test_update_from_csv_lookup(spark_session, pandas_df_to_temporary_csv):
    map_df = spark_session.createDataFrame(
        data=[
            (1, "i", "B", "original", "converted"),
            (2, "i", "A", "original", "converted"),
            (2, "i", "B", "original", "converted"),
            (3, "i", "B", "original", "converted"),
            (4, "i", "B", "original", "converted"),
            (5, "i", "A", "something else", "converted"),
            (6, "i", "A", "value", "converted A from value"),
            (6, "i", "B", None, "converted B from null"),
            (6, "i", "C", None, "converted C from null"),  # non existent column should added and then removed
            (None, "i", "B", None, "null"),  # missing id should be joined but not updated
            (7, "i", "B", None, "converted B from null"),  # non existent id should not be joined
        ],
        schema="""id integer, dataset_name string, target_column_name string, old_value string, new_value string""",
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

    output_df = update_from_csv_lookup(df, map, "i", "id")
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True)
