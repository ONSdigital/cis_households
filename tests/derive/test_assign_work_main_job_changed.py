from chispa import assert_df_equality

from cishouseholds.derive import assign_work_main_job_changed


def test_assign_work_main_job_changed(spark_session):
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, 1, 0, 0, "Yes"),
            (1, 1, 2, 0, "Yes"),
            (1, 1, 2, 0, "No")
        ],
        # fmt: on
        schema="""
        id integer,
        a integer,
        b integer,
        c integer,
        result string
        """,
    )

    input_df = expected_df.drop("result")

    output_df = assign_work_main_job_changed(
        df=input_df,
        column_name_to_assign="result",
        participant_id_column="id",
        reference_columns=["a", "b", "c"],
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
