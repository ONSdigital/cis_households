from chispa import assert_df_equality

from cishouseholds.edit import update_from_csv_lookup


def test_update_from_csv_lookup(spark_session, pandas_df_to_temporary_csv):
    map_df = spark_session.createDataFrame(
        data=[
            (1, "B", "val1", "vala"),
            (2, "A", "val3", "valz"),
            (2, "B", "val2", "valb"),
            (3, "B", "val3", "valc"),
            (4, "B", "val4", "vald"),
            (5, "A", "val5", "vale"),
            (6, "A", "val6", "velf"),
        ],
        schema="""id integer, column string, old string, new string""",
    )
    csv_filepath = pandas_df_to_temporary_csv(map_df.toPandas()).as_posix()
    input_df = spark_session.createDataFrame(
        data=[(1, 1, "val1"), (2, "val3", "val2"), (3, 1, "val3"), (4, "val4", "val4"), (5, 3, "val5")],
        schema="""id integer, A string, B string""",
    )
    expected_df = spark_session.createDataFrame(
        data=[(1, 1, "vala"), (2, "valz", "valb"), (3, 1, "valc"), (4, "val4", "vald"), (5, 3, "val5")],
        schema="""id integer, A string, B string""",
    )
    df = input_df
    map = csv_filepath

    output_df = update_from_csv_lookup(df, map, "id")
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True)
