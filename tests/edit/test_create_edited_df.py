from chispa import assert_df_equality

from cishouseholds.edit import create_edited_df


def test_create_edited_df(spark_session):
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
        # fmt: off
        data=[
            (1, "original", "original", None, None, "original", "converted", None, None),
            (2, "original", "original", "original", "converted", "original", "converted", None, None),
            (3, "original", "original", None, None, "original", "converted", None, None),
            (4, "original", "original", None, None, "original", "converted", None, None),
            (5, "original", "original", "something else", "converted", None, None, None, None),
            (6,"value",None,"value","converted A from value",None,"converted B from null",None,"converted C from null"),
        ],
        # fmt: on
        schema="""id integer, A string, B string,  A_old_value string, A_new_value string, B_old_value string, B_new_value string, C_old_value string, C_new_value string""",
    )
    output_df, csv = create_edited_df(input_df, map_df, "id")
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True)
