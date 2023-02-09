from chispa import assert_df_equality

from cishouseholds.edit import update_work_main_job_changed


def test_update_work_main_job_changed(spark_session):
    schema = """
        ref integer,
        id integer,
        a integer,
        b integer,
        c integer,
        d integer,
        changed string
        """
    input_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, 1, None, None, None,  1,       None),
            (2, 1, 1,    0,    0,     1,       "No"),
            (3, 1, 1,    2,    0,     2,       None),
            (4, 1, 2,    2,    0,     None,    "No"),
            (5, 1, None, None, None,  3,       "Yes"),
            (6, 1, None, None, None,  5,       "No")
        ],
        # fmt: on
        schema=schema
    )

    greedy_expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, 1, None, None, None, 1,        "Yes"), # first row and none null response
            (2, 1, 1,    0,    0,    1,        "Yes"), # d hasn't changed
            (3, 1, 1,    2,    0,    2,        "Yes"),
            (4, 1, 2,    2,    0,    None,     "Yes"), # only d has changed
            (5, 1, None, None, None, 3,        "Yes"),
            (6, 1, None, None, None, 5,        "Yes")
        ],
        # fmt: on
        schema=schema
    )

    minimal_expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, 1, None, None, None, 1,        "No"), # first row and none null response
            (2, 1, 1,    0,    0,    1,        "Yes"), # d hasn't changed
            (3, 1, 1,    2,    0,    2,        "No"),
            (4, 1, 2,    2,    0,    None,     "Yes"), # only d has changed
            (5, 1, None, None, None, 3,        "Yes"),
            (6, 1, None, None, None, 5,        "No")
        ],
        # fmt: on
        schema=schema
    )

    # greedy_output_df = update_work_main_job_changed(
    #     df=input_df,
    #     column_name_to_update="changed",
    #     participant_id_column="id",
    #     change_to_not_null_columns=[
    #         "a",
    #         "b",
    #     ],
    #     change_to_any_columns=["d"],
    # )

    minimal_output_df = update_work_main_job_changed(
        df=input_df,
        column_name_to_update="changed",
        participant_id_column="id",
        change_to_not_null_columns=["a"],
    )

    # assert_df_equality(greedy_output_df, greedy_expected_df, ignore_nullable=True, ignore_row_order=False)
    assert_df_equality(minimal_output_df, minimal_expected_df, ignore_nullable=True, ignore_row_order=False)
