from chispa import assert_df_equality

from cishouseholds.impute import fill_backwards_work_status_v2
from cishouseholds.impute import fill_forward_from_last_change
from cishouseholds.impute import fill_forward_only_to_nulls
from cishouseholds.impute import fill_forward_only_to_nulls_in_dataset_based_on_column


def test_fill_forward_from_last_change(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # TODO: incorporate survey_response_type, not fill forward None
            ## understand how change column is derived
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
                (5, "2020-08-16",   None,       None,   None,   None),

                (6, "2020-08-15",   "No",       None,   5,      None), # it should fill forward first record
                (6, "2020-08-16",   None,       None,   10,     None),
                (6, "2020-08-16",   None,       None,   None,   None),

                (7, "2020-08-14",   "No",       None,   None,   None), # id 6 should fill forward to Nulls
                (7, "2020-08-15",   "No",       None,   1,      None),
                (7, "2020-08-16",   None,       None,   None,   None),

                (8, "2020-01-01",   None,       1,      2,      3),
                (8, "2020-01-02",   "No",       1,      2,      3),
                (8, "2020-01-02",   "No",       1,      2,      3),
            # duplicated window
            # fmt: on
        ],
        schema="id integer, date string, changed string, var_1 integer, var_2 integer, var_3 integer",
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
                (5, "2020-08-16",   None,       None,   5,      None),

                (6, "2020-08-15",   "No",       None,   5,      None),
                (6, "2020-08-16",   None,       None,   5,      None),
                (6, "2020-08-16",   None,       None,   5,      None),

                (7, "2020-08-14",   "No",       None,   None,   None),
                (7, "2020-08-15",   "No",       None,   None,   None),
                (7, "2020-08-16",   None,       None,   None,   None),

                (8, "2020-01-01",   None,       1,      2,      3),
                (8, "2020-01-02",   "No",       1,      2,      3),
                (8, "2020-01-02",   "No",       1,      2,      3),
            # fmt: on
        ],
        schema="id integer, date string, changed string, var_1 integer, var_2 integer, var_3 integer",
    )
    actual_df = fill_forward_from_last_change(
        df=input_df,
        fill_forward_columns=["var_1", "var_2", "var_3"],
        participant_id_column="id",
        visit_date_column="date",
        record_changed_column="changed",
        record_changed_value="Yes",
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)


def test_fill_forward_only_to_nulls_in_dataset_based_on_column(spark_session):
    schema = """
            id integer,
            date string,
            var_1 string, var_2 string, var_3 string,
            changed string,
            dataset integer
        """
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                # fill_forward should happen as from v1 to v2.
                (1,     '2020-01-04',      'g1',   'g2',   'g3',    'Yes',      1),
                (1,     '2020-01-05',      None,   None,   None,    None,       2), # fill_forward
                (1,     '2020-01-06',      None,   'r2',   None,    0,          2),
                (1,     '2020-01-07',      None,   None,   None,    0,          2),
                (1,     '2020-01-08',      'f1',   'f2',   'f3',    'Yes',      2),
                (1,     '2020-01-09',      None,   None,   None,    0,          2),
                (1,     '2020-01-10',      None,   None,   None,    0,          2),

                (1,     '2020-01-11',      't1',   None,   None,    'Yes',      1),
                (1,     '2020-01-12',      None,   'a1',   'h1',    'No',       2), # only fill_forward var_1
                (1,     '2020-01-13',      None,   None,   None,    'No',       2), # fill_forward

                (2,     '2020-01-01',      None,   None,   None,    0,          1), # not fill_forward

                (3,     '2020-01-01',      'g1',   'g2',   'g3',    'Yes',      1),
                (3,     '2020-01-02',      None,   None,   None,    None,       1), # not fill_forward as from v1 to v1

                (4,     '2020-01-01',      'g1',   'g2',   'g3',    'Yes',      2),
                (4,     '2020-01-02',      None,   None,   None,    None,       2),
            # fill_forward as from v2 to v2
            # fmt: on
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1,     '2020-01-04',      'g1',   'g2',   'g3',    'Yes',      1),
                (1,     '2020-01-05',      'g1',   'g2',   'g3',    None,       2),
                (1,     '2020-01-06',      'g1',   'r2',   'g3',    0,          2),
                (1,     '2020-01-07',      'g1',   'r2',   'g3',    0,          2),
                (1,     '2020-01-08',      'f1',   'f2',   'f3',    'Yes',      2),
                (1,     '2020-01-09',      'f1',   'f2',   'f3',    0,          2),
                (1,     '2020-01-10',      'f1',   'f2',   'f3',    0,          2),

                (1,     '2020-01-11',      't1',   None,   None,    'Yes',      1),
                (1,     '2020-01-12',      't1',   'a1',   'h1',    'No',       2),
                (1,     '2020-01-13',      't1',   'a1',   'h1',    'No',       2),

                (2,     '2020-01-01',      None,   None,   None,    0,          1),

                (3,     '2020-01-01',      'g1',   'g2',   'g3',    'Yes',      1),
                (3,     '2020-01-02',      None,   None,   None,    None,       1),

                (4,     '2020-01-01',      'g1',   'g2',   'g3',    'Yes',      2),
                (4,     '2020-01-02',      'g1',   'g2',   'g3',    None,       2),
            # fmt: on
        ],
        schema=schema,
    )
    actual_df = fill_forward_only_to_nulls_in_dataset_based_on_column(
        df=input_df,
        id="id",
        date="date",
        changed="changed",
        dataset="dataset",
        dataset_value=2,
        list_fill_forward=["var_1", "var_2", "var_3"],
        changed_positive_value="Yes",
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)


def test_fill_backwards_work_status_v2(spark_session):
    schema = """id integer, date string, condition_col integer, edit_col integer"""

    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (3, "1020-01-01",   1,      None), # outside of cutoff date

                (3, "2021-01-01",   None,   None), # id=3 fill backwards case
                (3, "2021-01-02",   1,      2),
                (3, "2021-01-03",   None,   None),
                (3, "2021-01-04",   1,      1),
                (3, "2021-01-05",   None,   None),
                (3, "2021-01-06",   None,   2),

                (3, "2021-01-07",   None,   None),
                (3, "2100-01-01",   None,   88), # outside of cutoff date

                (4, "2021-01-05",   None,   None),

                (5, "2021-01-01",   None,   None), # id=5 NOT fill backwards case
                (5, "2021-01-02",   1,      2),
                (5, "2021-01-02",   None,   None),
                (5, "2021-01-03",   3,      4),
                (5, "2021-01-04",   None,   None),
                (5, "2021-01-05",   None,   None),

                (6, "2021-01-01",   None,   None),
                (6, "2021-01-02",   1,      2),
                (6, "2021-01-03",   None,   None),
                (6, "2021-01-04",   1,      1),
                (6, "2021-01-05",   None,   None),
                (6, "2021-01-06",   None,   None), # id=3 NOT fill backwards case
                (6, "2021-01-07",   None,   44),
            # fmt: on
        ],
        schema=schema,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (3, "1020-01-01",   1,      None),

                (3, "2021-01-01",   None,   2), # filled backward
                (3, "2021-01-02",   1,      2),
                (3, "2021-01-03",   None,   1), # filled backward
                (3, "2021-01-04",   1,      1),
                (3, "2021-01-05",   None,   2), # filled backward
                (3, "2021-01-06",   None,   2),

                (3, "2021-01-07",   None,   None),
                (3, "2100-01-01",   None,   88),

                (4, "2021-01-05",   None,   None),

                (5, "2021-01-01",   None,   None),
                (5, "2021-01-02",   1,      2),
                (5, "2021-01-02",   None,   None),
                (5, "2021-01-03",   3,      4),
                (5, "2021-01-04",   None,   None),
                (5, "2021-01-05",   None,   None),

                (6, "2021-01-01",   None,   2),
                (6, "2021-01-02",   1,      2),
                (6, "2021-01-03",   None,   1),
                (6, "2021-01-04",   1,      1),
                (6, "2021-01-05",   None,   None),
                (6, "2021-01-06",   None,   None), # id=3 NOT fill backwards case
                (6, "2021-01-07",   None,   44),
            # 44 not wanted to be filled backwards
            # fmt: on
        ],
        schema=schema,
    )
    actual_df = fill_backwards_work_status_v2(
        df=input_df,
        date="date",
        id="id",
        fill_backward_column="edit_col",
        condition_column="condition_col",
        condition_column_values=[1],
        date_range=["2019-01-01", "2030-01-01"],
        fill_only_backward_column_values=[1, 2, 4, 88],
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)


def test_fill_forward_only_to_nulls(spark_session):
    schema = """
            id integer,
            date string,
            var_1 string, var_2 string, var_3 string,
            changed string,
            dataset integer
    """
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1,     '2020-01-04',      'g1',   'g2',   'g3',    'Yes',      1),
                (1,     '2020-01-05',      None,   None,   None,    None,       2), # fill_forward
                (1,     '2020-01-06',      None,   'r2',   None,    0,          2),
                (1,     '2020-01-07',      None,   None,   None,    0,          2),
                (1,     '2020-01-08',      'f1',   'f2',   'f3',    'Yes',      2),
                (1,     '2020-01-09',      None,   None,   None,    0,          2),
                (1,     '2020-01-10',      None,   None,   None,    0,          2),

                (1,     '2020-01-11',      't1',   None,   None,    'Yes',      1),
                (1,     '2020-01-12',      None,   'a1',   'h1',    'No',       2), # only fill_forward var_1
                (1,     '2020-01-13',      None,   None,   None,    'No',       2), # fill_forward

                (2,     '2020-01-01',      None,   None,   None,    0,          1), # not fill_forward

                (3,     '2020-01-01',      'g1',   'g2',   'g3',    'Yes',      1),
                (3,     '2020-01-02',      None,   None,   None,    None,       1), # not fill_forward as from v1 to v1

                (4,     '2020-01-01',      'g1',   'g2',   'g3',    'Yes',      2),
                (4,     '2020-01-02',      None,   None,   None,    None,       2),
            # fmt: on
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1,     '2020-01-04',      'g1',   'g2',   'g3',    'Yes',      1),
                (1,     '2020-01-05',      'g1',   'g2',   'g3',    None,       2),
                (1,     '2020-01-06',      'g1',   'r2',   'g3',    0,          2),
                (1,     '2020-01-07',      'g1',   'r2',   'g3',    0,          2),
                (1,     '2020-01-08',      'f1',   'f2',   'f3',    'Yes',      2),
                (1,     '2020-01-09',      'f1',   'f2',   'f3',    0,          2),
                (1,     '2020-01-10',      'f1',   'f2',   'f3',    0,          2),

                (1,     '2020-01-11',      't1',   'f2',   'f3',    'Yes',      1),
                (1,     '2020-01-12',      't1',   'a1',   'h1',    'No',       2),
                (1,     '2020-01-13',      't1',   'a1',   'h1',    'No',       2),

                (2,     '2020-01-01',      None,   None,   None,    0,          1),

                (3,     '2020-01-01',      'g1',   'g2',   'g3',    'Yes',      1),
                (3,     '2020-01-02',      'g1',   'g2',   'g3',    None,       1),

                (4,     '2020-01-01',      'g1',   'g2',   'g3',    'Yes',      2),
                (4,     '2020-01-02',      'g1',   'g2',   'g3',    None,       2),
            # fmt: on
        ],
        schema=schema,
    )
    actual_df = fill_forward_only_to_nulls(
        df=input_df,
        id="id",
        date="date",
        list_fill_forward=["var_1", "var_2", "var_3"],
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
