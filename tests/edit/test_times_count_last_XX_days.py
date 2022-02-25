from chispa import assert_df_equality

from cishouseholds.edit import count_activities_last_XX_days


def test_count_activities_last_XX_days(spark_session):
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
                (1,     3,      2,      1,      0), # ensure no action is taken if combination is not null
                (2,     None,   2,      1,      1), # sum happens but isnt larger than the max_value
                (3,     None,   5,      6,      1), # sum happens but it is larger than max_value
                (4,     None,   1,      None,   None), # if some null ensure that the remaining are summed.
                (5,     None,   None,   None,   None),
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
                (3,     10,     5,      6,      1),
                (4,     1,      1,      None,   None),
                (5,     None,   None,   None,   None),
            # fmt: on
        ],
        schema=schema,
    )
    output_df = count_activities_last_XX_days(
        df=input_df,
        activity_combo_last_XX_days="activity_combo",
        list_activities_last_XX_days=[
            "activity_a",
            "activity_b",
            "activity_c",
        ],
        max_value=10,
    )
    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True)
