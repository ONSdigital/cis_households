from chispa import assert_df_equality

from cishouseholds.derive import assign_people_in_household_count
from cishouseholds.edit import update_participant_not_consented


def test_assign_people_in_household_count(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # All i's are 0, should not count any of them
            (1, 0, 0, 0, 1, 1, 1, 7),
            # Exception i not 0, but should still not count any of the i's
            (1, 0, 0, 20, 1, 1, 1, 7),
            # Some i's not 0, so should include i 0's in count
            (1, 1, 0, 0, 1, 1, 1, 7),
            # 0 in p, should be counted
            (1, None, None, None, 1, 0, 1, 3),
            # All i Null
            (3, None, None, None, 1, 0, 0, 4),
            # Null in non_consent, shouldn't be counted and outcome should not be null
            (1, None, None, None, 2, None, None, 2),
        ],
        schema="count integer, i1 integer, i2 integer, i3 integer, p1 integer, p2 integer, non_consent integer, total_count integer",
    )
    input_df = expected_df.drop("total_count")

    output_df = assign_people_in_household_count(
        input_df, "total_count", r"i[1-3]", r"i[1,2]", r"p[1-2]", "count", "non_consent"
    )
    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )


def test_assign_people_in_household_count_integration(spark_session):
    # fmt: off
    expected_df = spark_session.createDataFrame(
        data=[(1, 2, 0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 3, None, None, None, None, None, None, None,3),
                (2, 3, 0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 16, None, None, None, None, None, None, None,4),
                (3, 4, None, None, 0, 0, 0, None, 0, None, 0, None, None, None, None, None, None, None, None, 0, 0, 0, 0, 0, 0, 0, 0,4),
                (4, 2, None, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,2),
                (5, 1, None, 0, 0, 0, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,3),
        ],
        schema="id integer, participant_count integer, non_consent_count integer, i1 integer, i2 integer, i3 integer, i4 integer, i5 integer, i6 integer, i7 integer, i8 integer, p1 integer, p2 integer, p3 integer, p4 integer, p5 integer, p6 integer, p7 integer, p8 integer, pn1 integer,pn2 integer, pn3 integer, pn4 integer, pn5 integer, pn6 integer, pn7 integer, pn8 integer, total_count integer",
    )
    # fmt: on
    expected_df = update_participant_not_consented(expected_df, "non_consent_count", r"pn[1-8]")
    input_df = expected_df.drop("total_count")
    output_df = assign_people_in_household_count(
        input_df, "total_count", r"i[1-8]", r"i[1-5,7,8]", r"p[1-8]", "participant_count", "non_consent_count"
    )
    assert_df_equality(output_df, expected_df, ignore_row_order=False, ignore_column_order=True, ignore_nullable=True)
