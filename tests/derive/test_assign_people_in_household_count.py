from chispa import assert_df_equality

from cishouseholds.derive import assign_people_in_household_count


def test_assign_people_in_household_count(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (3, 1, None, None, 1, 0, 1, 7),
            (3, 1, 1, 1, 1, 0, 1, 9),
            (3, 0, 0, 0, 1, 0, 1, 9),
        ],
        schema="count integer, i1 integer, i2 integer, i3 integer, p1 integer, p2 integer, non_consent integer, total_count integer",
    )
    input_df = expected_df.drop("total_count")

    output_df = assign_people_in_household_count(
        input_df, "total_count", r"i\d{1,}", r"i[1,3]", r"p\d{1,}", "count", "non_consent"
    )
    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
