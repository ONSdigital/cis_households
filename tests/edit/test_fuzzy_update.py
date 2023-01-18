from chispa.dataframe_comparer import assert_df_equality

from cishouseholds.edit import fuzzy_update


def test_fuzzy_update(spark_session):
    # fmt: off
    input_df = spark_session.createDataFrame(
        data=[
            (1, "1", "A",  "L", 1, None),
            (1, "3", "A",  "L", 2, None),
            (1, "4", None, "L", 3, None),
            (1, "4", "A", None, 3, "update"),
            (1, "3", "B", "L", 2, "update"),
            (1, "5", "A", "L", 2, "update")
        ],
        schema="""id integer, A string, B string, C string, D integer, update string""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "1", "A",  "L",  1, None), # no rows exist with more than 2 matches and a not null val in update col
            (1, "3", "A",  "L",  2, "update"),
            (1, "4", None, "L",  3, None), # no rows exist with more than 2 matches and a not null val in update col
            (1, "4", "A",  None, 3, "update"),
            (1, "3", "B",  "L",  2, "update"),
            (1, "5", "A",  "L",  2, "update"),
        ],
        schema="""id integer, A string, B string, C string, D integer, update string""",
    )
    # fmt: on

    output_df = fuzzy_update(
        input_df, cols_to_check=["A", "B", "C", "D"], min_matches=2, update_column="update", id_column="id"
    )
    assert_df_equality(expected_df, output_df, ignore_row_order=True, ignore_column_order=True)
