from chispa import assert_df_equality

from cishouseholds.impute import fill_forwards_overriding_not_nulls


def test_fill_backwards_lattest_version(spark_session):
    input_data_v1 = [
        # fmt:off
            (1,     1,     '2020-01-01',       'm',        '1990-01-01',       'white'),
            (1,     2,     '2020-01-02',       'm',        '1990-01-01',       'white'),

            (2,     3,     '2020-01-02',       'm',        '1990-01-01',       'white'),
        # fmt:on
    ]
    input_data_v2 = [
        # fmt:off
            (1,     4,     '2020-01-01',       'm',        '1990-01-01',       'white'),
            (1,     5,     '2020-01-02',       'm',        '1990-01-01',       'black'),

            (2,     6,     '2020-01-02',       'f',        '1990-01-01',       'white'),
        # fmt:on
    ]

    expected_data = [
        # fmt:off
            (1,     1,     '2020-01-01',       'm',        '1990-01-01',       'black'),
            (1,     2,     '2020-01-02',       'm',        '1990-01-01',       'black'),
            (1,     4,     '2020-01-01',       'm',        '1990-01-01',       'black'),
            (1,     5,     '2020-01-02',       'm',        '1990-01-01',       'black'),

            (2,     3,     '2020-01-02',       'f',        '1990-01-01',       'white'),
            (2,     6,     '2020-01-02',       'f',        '1990-01-01',       'white'),
        # fmt:on
    ]
    schema = """
            participant_id integer,
            visit_id integer,
            visit_date string,
            sex string,
            dob string,
            ethnicity string
            """

    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)
    input_df_v1 = spark_session.createDataFrame(data=input_data_v1, schema=schema)
    input_df_v2 = spark_session.createDataFrame(data=input_data_v2, schema=schema)

    output_df = fill_forwards_overriding_not_nulls(
        df=[input_df_v1, input_df_v2],
    )
    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
