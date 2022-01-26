from chispa import assert_df_equality

from cishouseholds.impute import fill_forward_work_columns


# debating whether to fix seed or mock F.rand
def test_fill_forward_work_columns(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, "2020/11/11", "Yes", None, None, 2),
            (2, "2020/10/15", "No", 2, None, 3),
            (3, "2020/05/21", "No", 1, 2, 3),
            (3, "2020/05/27", "Yes", 2, None, 2),
            (3, "2020/07/20", "No", None, 1, None),
            (3, "2020/08/13", "No", 1, None, 2),
        ],
        schema="id integer, date string, changed string, work_1 integer, work_2 integer, work_3 integer",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2020/11/11", "Yes", None, None, 2),
            (3, "2020/05/21", "No", 1, 2, 3),
            (3, "2020/05/27", "Yes", 2, None, 2),
            (3, "2020/07/20", "No", 1, 2, 3),
            (3, "2020/08/13", "No", 1, 2, 3),
            (2, "2020/10/15", "No", 2, None, 3),
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
    actual_df.toPandas().to_csv("out.csv", index=False)
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
