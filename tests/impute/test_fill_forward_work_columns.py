from chispa import assert_df_equality

from cishouseholds.impute import fill_forward_work_columns


def test_fill_forward_work_columns(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, "2020/11/11", "Yes", 1, 1, 1),
            (2, "2020/10/15", "No", None, None, None),
            (3, "2020/05/21", "Yes", 1, 2, 3),
            (3, "2020/05/27", "No", None, None, None),
            (3, "2020/07/20", "Yes", 3, 2, 1),
            (3, "2020/08/13", "No", None, None, None),
        ],
        schema="id integer, date string, changed string, work_1 integer, work_2 integer, work_3 integer",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2020/11/11", "Yes", 1, 1, 1),
            (2, "2020/10/15", "No", None, None, None),
            (3, "2020/05/21", "Yes", 1, 2, 3),
            (3, "2020/05/27", "No", 1, 2, 3),
            (3, "2020/07/20", "Yes", 3, 2, 1),
            (3, "2020/08/13", "No", 3, 2, 1),
        ],
        schema="id integer, date string, changed string, work_1 integer, work_2 integer, work_3 integer",
    )

    actual_df = fill_forward_work_columns(
        input_df,
        fill_forward_columns=["work_1", "work_2", "work_3"],
        participant_id_column="id",
        visit_date_column="date",
        main_job_changed_column="changed",
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
