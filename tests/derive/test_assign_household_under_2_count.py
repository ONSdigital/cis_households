from chispa import assert_df_equality

from cishouseholds.derive import assign_household_under_2_count


def test_assign_household_under_2_count(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1,     'Yes',     1,      1,      None,     2),    # Assign count of ages > 0
            (2,     'Yes',     1,      0,      None,     2),    # Include 0 in count
            (3,     'Yes',     None,   None,   None,     0),    # Assign 0 by default
            (4,     'Yes',     0,      0,      0,        0),    # Treat as invalid if all 0

            (5,     'Yes',     0,      None,   0,        2),    # count 0's

            (6,     'No',      0,      None,   0,        0),    # look for 0's and None = count is 0
            (7,     'No',      None,   None,   0,        0),
            (8,     'No',      None,   None,   None,     0),
            (9,     'No',      None,   None,   1,        1),
            # fmt: on
        ],
        schema="""id integer, condition string, 1_age integer, 2_age integer, 3_age integer, count integer""",
    )
    input_df = expected_df.drop("count")

    output_df = assign_household_under_2_count(
        df=input_df, column_name_to_assign="count", column_pattern=r"[1-3]_age", condition_column="condition"
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
