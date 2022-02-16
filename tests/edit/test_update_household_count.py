from chispa import assert_df_equality

from cishouseholds.edit import update_household_count


def test_update_household_count(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (3, 1, None, None, 1, 0, 1),
            (3, 1, 1, 1, 1, 0, 1),
            (3, 0, 0, 0, 1, 0, 1),
        ],
        schema="count integer, i1 integer, i2 integer, i3 integer, p1 integer, p2 integer, non_consent integer",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (4, 1, None, None, 1, 0, 1),
            (6, 1, 1, 1, 1, 0, 1),
            (6, 0, 0, 0, 1, 0, 1),
        ],
        schema="count integer, i1 integer, i2 integer, i3 integer, p1 integer, p2 integer, non_consent integer",
    )

    output_df = update_household_count(input_df, "count", r"i\d{1,}", r"i[1,3]", r"p\d{1,}", "non_consent")
    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
