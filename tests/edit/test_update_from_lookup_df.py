from chispa import assert_df_equality

from cishouseholds.edit import update_from_lookup_df


def test_update_from_lookup_df(spark_session):
    map_df = spark_session.createDataFrame(
        # All fields are read as string from csv (mixed types)
        data=[
            ("1", None, "A", "original", "edited"),
            ("2", None, "B", "1", "2"),
            ("3", None, "A", "original", "edited"),
            ("3", None, "B", "10", "20"),
            ("3", None, "C", None, None),  # Non existent column just prints warning
            ("5", None, "B", None, None),  # Non existent id should not be joined
        ],
        schema="""id string, dataset_name string, target_column_name string, old_value string, new_value string""",
    )
    input_df = spark_session.createDataFrame(
        data=[
            ("1", "original", None),
            ("2", None, 1),
            ("3", "original", 10),
        ],
        schema="""id string, A string, B integer""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("1", "edited", None),
            ("2", None, 2),
            ("3", "edited", 20),
        ],
        schema="""id string, A string, B integer""",
    )

    output_df = update_from_lookup_df(input_df, map_df, id_column="id")
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True)
