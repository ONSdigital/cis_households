from chispa import assert_df_equality

from cishouseholds.edit import update_cohort_variable


def test_update_cohort_variable(spark_session, pandas_df_to_temporary_csv):
    map_df = spark_session.createDataFrame(
        data=[
            (1, "A", "new"),
            (2, "A", "new"),
            (3, "A", "new"),
            (4, "A", "new"),
            (5, "A", "something else"),
            (6, "A", "new"),
        ],
        schema="""participant_id integer, current_cohort string, new_cohort string""",
    )
    csv_filepath = pandas_df_to_temporary_csv(map_df.toPandas()).as_posix()
    input_df = spark_session.createDataFrame(
        data=[
            (1, "A", "original"),
            (2, "A", "original"),
            (3, "A", "original"),
            (4, "A", "original"),
            (5, "B", "original"),
        ],
        schema="""participant_id integer, current_cohort string, study_cohort string""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "A", "new"),
            (2, "A", "new"),
            (3, "A", "new"),
            (4, "A", "new"),
            (5, "B", "original"),
        ],
        schema="""participant_id integer, current_cohort string, study_cohort string""",
    )
    map = csv_filepath

    output_df = update_cohort_variable(input_df, map, "current_cohort", "study_cohort", "participant_id")
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
