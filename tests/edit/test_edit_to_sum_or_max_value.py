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
    input_df = spark_session.createDataFrame(
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
    expected_df = spark_session.createDataFrame(
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
    output_df = edit_to_sum_or_max_value(
        df=input_df,
        column_name_to_assign="activity_combo",
        columns_to_sum=[
            "activity_a",
            "activity_b",
            "activity_c",
        ],
        max_value=7,
    )
    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
