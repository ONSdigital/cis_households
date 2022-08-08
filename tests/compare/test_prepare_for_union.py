from chispa import assert_df_equality

from cishouseholds.prepare_for_union import prepare_for_union


def test_prepare_for_union(spark_session):

    example_ref = spark_session.createDataFrame(
        data=[
            ("ABC123", 1, "ABC7673", 1, 2, 2),
            ("ABC123", 1, "ABC7234", 1, 2, 2),
            ("ABC456", 1, "ABC7216", 1, 2, 1),
            ("ABC456", 1, "ABC7837", 1, 2, 1),
            ("ABC789", 1, "ABC7893", 1, 2, 2),
            ("ABC789", 1, "ABC7810", 1, 2, 2),
        ],
        schema="col_1 string, test_1 integer, test_2 string, \
               col_2 integer, col_3 integer, test_3 integer",
    )

    example_df = spark_session.createDataFrame(
        data=[
            (1, "ABC123", 1, 2, 6),
            (2, "ABC123", 2, 3, 5),
            (3, "ABC456", 1, 4, 4),
            (4, "ABC456", 2, 4, 3),
            (5, "ABC789", 1, 2, 2),
            (6, "ABC789", 2, 1, 1),
        ],
        schema="test_1 integer, test_2 string, new_col_1 integer, new_col_2 integer,test_3 integer",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (1, "ABC123", None, None, 6, 1, 2, None),
            (2, "ABC123", None, None, 5, 2, 3, None),
            (3, "ABC456", None, None, 4, 1, 4, None),
            (4, "ABC456", None, None, 3, 2, 4, None),
            (5, "ABC789", None, None, 2, 1, 2, None),
            (6, "ABC789", None, None, 1, 2, 1, None),
        ],
        schema="test_1 integer, test_2 string, \
                col_2 integer, col_3 integer, test_3 integer,new_col_1 integer, new_col_2 integer, col_1 string",
    )

    expected_ref = spark_session.createDataFrame(
        data=[
            (1, "ABC7673", 1, 2, 2, None, None, "ABC123"),
            (1, "ABC7234", 1, 2, 2, None, None, "ABC123"),
            (1, "ABC7216", 1, 2, 1, None, None, "ABC456"),
            (1, "ABC7837", 1, 2, 1, None, None, "ABC456"),
            (1, "ABC7893", 1, 2, 2, None, None, "ABC789"),
            (1, "ABC7810", 1, 2, 2, None, None, "ABC789"),
        ],
        schema="test_1 integer, test_2 string, \
            col_2 integer, col_3 integer, test_3 integer,  new_col_1 integer, new_col_2 integer, col_1 string",
    )

    output_dfs = prepare_for_union([example_df, example_ref])
    assert_df_equality(output_dfs[0], expected_df)
    assert_df_equality(output_dfs[1], expected_ref)
