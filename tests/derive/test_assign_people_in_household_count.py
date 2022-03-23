from pickle import NONE

from chispa import assert_df_equality

from cishouseholds.derive import assign_people_in_household_count


def test_assign_people_in_household_count(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # All i's are 0, should count all of them
            (1, 0, 0, 0, 1, 1, 1, 7),
            # Exception should not count nones but should still count all of the i's
            (1, None, None, 0, 1, 1, 1, 5),
            # One p should not be counted
            (1, 1, 0, 0, 1, None, 1, 6),
            # 0 in p, should be counted
            (1, None, None, None, 1, 0, 1, 4),
            # All i Null one p is Null
            (3, None, None, None, 0, None, 1, 5),
            # Null in non_consent, shouldn't be counted and outcome should not be null
            (1, None, None, None, 2, None, None, 2),
        ],
        schema="count integer, i1 integer, i2 integer, i3 integer, p1 integer, p2 integer, non_consent integer, total_count integer",
    )
    input_df = expected_df.drop("total_count")

    output_df = assign_people_in_household_count(input_df, "total_count", r"i[1-3]", r"p[1-2]", "count", "non_consent")
    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
