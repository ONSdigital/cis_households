from chispa import assert_df_equality

from cishouseholds.impute import fill_forward_work_columns


def test_fill_forward_work_columns(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1, "2020-11-11",   "Yes",      1,      1,      1), # id 1, 2 should not be modified

                (2, "2020-10-15",   "No",       None,   None,   None),

                (3, "2020-01-01",   "Yes",      1,      2,      3), # id 3 should have 2 fill forwards
                (3, "2020-01-02",   "No",       None,   None,   None),
                (3, "2020-01-03",   "Yes",      3,      2,      None),
                (3, "2020-01-04",   None,       None,   None,   None),
                (3, "2020-01-05",   None,       None,   None,   None),

                (4, "2020-08-14",   None,       3,      2,      1), # id 4 should have one fill forward only when ALL nulls
                (4, "2020-08-15",   "No",       None,   None,   None),
                (4, "2020-08-16",   None,       6,      7,      8),

                (5, "2020-08-15",   "No",       None,   5,      None),
                (5, "2020-08-16",   None,       None,   None,   None),

                (6, "2020-08-15",   "No",       None,   5,      None), # it should fill forward first record
                (6, "2020-08-16",   None,       None,   10,     None),
                (6, "2020-08-16",   None,       None,   None,   None),

                (7, "2020-08-14",   "No",       None,   None,   None), # id 6 should fill forward to Nulls
                (7, "2020-08-15",   "No",       None,   1,      None),
                (7, "2020-08-16",   None,       None,   None,   None),
            # fmt: on
        ],
        schema="id integer, date string, changed string, work_1 integer, work_2 integer, work_3 integer",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1, "2020-11-11",   "Yes",      1,      1,      1),

                (2, "2020-10-15",   "No",       None,   None,   None),

                (3, "2020-01-01",   "Yes",      1,      2,      3),
                (3, "2020-01-02",   "No",       1,      2,      3),
                (3, "2020-01-03",   "Yes",      3,      2,      None),
                (3, "2020-01-04",   None,       3,      2,      None),
                (3, "2020-01-05",   None,       3,      2,      None),

                (4, "2020-08-14",   None,       3,      2,      1),
                (4, "2020-08-15",   "No",       3,      2,      1),
                (4, "2020-08-16",   None,       3,      2,      1), # these values should be overrided

                (5, "2020-08-15",   "No",       None,   5,      None),
                (5, "2020-08-16",   None,       None,   5,      None),

                (6, "2020-08-15",   "No",       None,   5,      None),
                (6, "2020-08-16",   None,       None,   5,      None),
                (6, "2020-08-16",   None,       None,   5,      None),

                (7, "2020-08-14",   "No",       None,   None,   None),
                (7, "2020-08-15",   "No",       None,   None,   None),
                (7, "2020-08-16",   None,       None,   None,   None),
            # fmt: on
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
