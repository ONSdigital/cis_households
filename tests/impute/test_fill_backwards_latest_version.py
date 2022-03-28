from chispa import assert_df_equality

from cishouseholds.impute import fill_backwards_overriding_not_nulls


def test_fill_backwards_lattest_version(spark_session):
    input_data = [
        # fmt:off
            ("Voyager 1 original",      1,     1,     '2020-01-01',       'm',        '1990-01-01',       'white'),
            ("Voyager 1 original",      1,     2,     '2020-01-02',       'm',        '1990-01-01',       'white'),

            ("Voyager 1 original",      2,     3,     '2020-01-02',       'm',        '1990-01-01',       'white'),

            ("Voyager 2",               1,     4,     '2020-01-01',       'm',        '1990-01-01',       'white'),
            ("Voyager 2",               1,     5,     '2020-01-02',       'm',        '1990-01-01',       'black'),

            ("Voyager 2",               2,     6,     '2020-01-02',       'f',        '1990-01-01',       'white'),

            ("Voyager 1 original",      3,     6,     '2020-01-01',       'f',        '1990-01-01',       'white'),
            ("Voyager 2",               3,     6,     '2020-01-02',       'f',        '1990-01-10',       'white'),

            ("Voyager 1 original",      4,     6,     '2020-01-01',       'f',        '1990-01-01',       'white'),
            ("Voyager 2",               4,     6,     '2020-01-02',       None,       '1990-01-01',       'white'),
        # fmt:on
    ]

    expected_data = [
        # fmt:off
            ("Voyager 1 original",      1,     1,     '2020-01-01',       'm',        '1990-01-01',       'black'), # test to impute race
            ("Voyager 1 original",      1,     2,     '2020-01-02',       'm',        '1990-01-01',       'black'),
            ("Voyager 2",               1,     4,     '2020-01-01',       'm',        '1990-01-01',       'black'),
            ("Voyager 2",               1,     5,     '2020-01-02',       'm',        '1990-01-01',       'black'),

            ("Voyager 1 original",      2,     3,     '2020-01-02',       'f',        '1990-01-01',       'white'), # test to do nothing
            ("Voyager 2",               2,     6,     '2020-01-02',       'f',        '1990-01-01',       'white'),

            ("Voyager 1 original",      3,     6,     '2020-01-01',       'f',        '1990-01-10',       'white'), # test to impute dob
            ("Voyager 2",               3,     6,     '2020-01-02',       'f',        '1990-01-10',       'white'),

            ("Voyager 1 original",      4,     6,     '2020-01-01',       'f',        '1990-01-01',       'white'), # should not impute None
            ("Voyager 2",               4,     6,     '2020-01-02',       None,       '1990-01-01',       'white'),
        # fmt:on
    ]
    schema = """
            dataset string,
            participant_id integer,
            visit_id integer,
            visit_date string,
            sex string,
            dob string,
            ethnicity string
            """

    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)
    input_df = spark_session.createDataFrame(data=input_data, schema=schema)

    df = fill_backwards_overriding_not_nulls(
        df=input_df,
        column_identity="participant_id",
        ordering_column="visit_date",
        dataset_column="dataset",
        column_list=["sex", "dob", "ethnicity"],
    )
    assert_df_equality(df, expected_df, ignore_row_order=True, ignore_column_order=True)
