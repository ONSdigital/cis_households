from chispa import assert_df_equality

from cishouseholds.edit import update_from_lookup_df


def test_update_from_lookup_df(spark_session):
    map_df = spark_session.createDataFrame(
        # All fields are read as string from csv (mixed types)
        data=[
            ("id1", "1", None, "A", "original", "edited"),
            ("id1", "2", None, "B", "1", "2"),
            ("id1", "3", None, "A", "original", "edited"),
            ("id1", "3", None, "B", "10", "20"),
            ("id1", "3", None, "D", None, None),  # Non existent column just prints warning
            ("id1", "5", None, "B", None, None),  # Non existent id should not be joined
            ("id2", "A", None, "C", 0, 1),  # edits multiple rows given same id
        ],
        schema="""id_column_name string, id string, dataset_name string, target_column_name string, old_value string, new_value string""",
    )
    schema = "id1 string, id2 string, A string, B integer, C integer"
    input_df = spark_session.createDataFrame(
        data=[
            ("1", "A", "original", None, None),
            ("2", "A", None, 1, 0),
            ("3", "A", "original", 10, 0),
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("1", "A", "edited", None, None),
            ("2", "A", None, 2, 1),
            ("3", "A", "edited", 20, 1),
        ],
        schema=schema,
    )

    output_df = update_from_lookup_df(input_df, map_df)
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True)
