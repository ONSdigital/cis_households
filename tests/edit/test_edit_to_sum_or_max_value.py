from chispa import assert_df_equality

from cishouseholds.edit import edit_to_sum_or_max_value


def test_edit_to_sum_or_max_value(spark_session):
    schema = """
        id integer,
        activity_combo integer,
        activity_a integer,
        activity_b integer,
        activity_c integer
    """
    input_df1 = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1,     3,    2,    1,    0), # ensure value not edited if combination is not null
                (2,     None, 2,    1,    1), # sum is lower than the max_value
                (3,     None, 5,    7,    1), # sum is larger than max_value, so max value used
                (4,     None, 1,    None, None), # if some null ensure that the remaining are summed.
                (5,     None, None, None, None),
            # if all null should give null
            # fmt: on
        ],
        schema=schema,
    )
    schema2 = """
            id integer,
            activity_combo integer,
            activity_a string,
            activity_b string,
            activity_c string
        """
    input_df2 = spark_session.createDataFrame(
        data=[
            # fmt: off
            (6, None, "5", None, None),  # value cast to integer so that sum > max_value
            (7, None, "1", "2",  "1"),
            (8, None, "don't know", 0, "don't know"),
            # fmt: on
        ],
        schema=schema2,
    )
    expected_df1 = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1,     3,      2,      1,      0),
                (2,     4,      2,      1,      1),
                (3,     7,      5,      7,      1),
                (4,     1,      1,      None,   None),
                (5,     None,   None,   None,   None),
            # fmt: on
        ],
        schema=schema,
    )
    expected_df2 = spark_session.createDataFrame(
        data=[
            # fmt: off
            (6, 5,  5, None, None),
            (7, 4,  1, 2,    1),
            (8, 0, "don't know",0,"don't know"),
            # fmt: on
        ],
        schema=schema2,
    )
    output_df1 = edit_to_sum_or_max_value(
        df=input_df1,
        column_name_to_update="activity_combo",
        columns_to_sum=[
            "activity_a",
            "activity_b",
            "activity_c",
        ],
        max_value=7,
    )
    output_df2 = edit_to_sum_or_max_value(
        df=input_df2,
        column_name_to_update="activity_combo",
        columns_to_sum=[
            "activity_a",
            "activity_b",
            "activity_c",
        ],
        max_value=7,
    )
    assert_df_equality(expected_df1, output_df1, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
    assert_df_equality(expected_df2, output_df2, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
