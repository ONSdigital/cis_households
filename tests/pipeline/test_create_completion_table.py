from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import create_completion_table


def test_match_type(spark_session):
    schema = """
        id integer,
        start string,
        status string
    """

    input_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, "2021-01-01", "Submitted"),
            (1, "2021-01-01", "Submitted"),
            (2, "2021-01-01", "Completed"),
            (3, "2021-01-02", "Submitted"),
        ],
        # fmt: on
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            ("2021-01-01", 0.5,0.5),
            ("2021-01-02",1.0,0.0),
        ],
        # fmt: on
        schema="start string, daily_full_completion_rate double, daily_partial_completion_rate double",
    )

    output_df = create_completion_table(
        df=input_df,
        participant_id_column="id",
        window_start_column="start",
        window_status_column="status"
    )
    output_df.show()
    assert_df_equality(output_df, expected_df)
