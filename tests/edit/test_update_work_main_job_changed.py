from chispa import assert_df_equality

from cishouseholds.edit import update_work_main_job_changed


def test_update_work_main_job_changed(spark_session):
    schema = """
        id integer,
        a integer,
        b integer,
        c integer,
        changed string
        """
    input_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, None, None, None,  None),
            (1, 1,    0,    0,    "Yes"),
            (1, 1,    2,    0,     None),
            (1, 1,    2,    0,     "No"),
            (1, None, None, None,  "Yes"),
            (1, None, None, None,  None)
        ],
        # fmt: on
        schema=schema
    )

    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, None, None, None, "No"),
            (1, 1,    0,    0,    "Yes"),
            (1, 1,    2,    0,    "Yes"),
            (1, 1,    2,    0,    "No"),
            (1, None, None, None, "Yes"),
            (1, None, None, None, "No")
        ],
        # fmt: on
        schema=schema
    )

    output_df = update_work_main_job_changed(
        df=input_df,
        column_name_to_update="changed",
        participant_id_column="id",
        reference_columns=["a", "b", "c"],
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
