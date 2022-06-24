from chispa import assert_df_equality

from cishouseholds.edit import update_from_lookup_df


def test_update_from_lookup_df(spark_session):
    map_df = spark_session.createDataFrame(
        # All fields are read as string from csv (mixed types)
        data=[
            ("id2", "1", None, "A", "original", "edited"),
            ("id2", "2", None, "B", "1", "2"),
            ("id2", "3", None, "A", "original", "edited"),
            ("id2", "3", None, "B", "10", "20"),
            ("id2", "5", None, "B", "20", "10"),  # convert to 20 to then become 30 on second pass
            ("id2", "3", None, "C", None, None),  # Non existent column just prints warning
            ("id2", "69", None, "B", None, None),  # Non existent id should not be joined
            ("id1", "3", None, "B", "10", "30"),  # Non existent id should not be joined
        ],
        schema="""id_column string, id string, dataset_name string, target_column_name string, old_value string, new_value string""",
    )
    input_df = spark_session.createDataFrame(
        data=[
            ("3", "1", "original", None),
            ("3", "2", None, 1),
            ("3", "3", "original", 20),
            ("3", "4", "original", 10),
            ("3", "5", "original", 20),
            ("3", "6", "original", 10),
        ],
        schema="id1 string, id2 string, A string, B integer",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("3", "1", "edited", None),
            ("3", "2", None, 2),
            ("3", "3", "edited", 20),
            ("3", "4", "original", 30),
            ("3", "5", "original", 30),
            ("3", "6", "original", 30),
        ],
        schema="id1 string, id2 string, A string, B integer",
    )

    output_df = update_from_lookup_df(input_df, map_df)
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True)
