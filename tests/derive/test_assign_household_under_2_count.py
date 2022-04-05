from chispa import assert_df_equality

from cishouseholds.derive import assign_household_under_2_count


def test_assign_household_under_2_count(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, 1, None),
            (1, 0, None),
            (None, None, None),
            (0, 0, 0),
        ],
        schema="""1_age integer, 2_age integer, 3_age integer""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (2, 1, 1, None),  # Assign count of ages > 0
            (2, 1, 0, None),  # Include 0 in count
            (0, None, None, None),  # Assign 0 by default
            (0, 0, 0, 0),  # Treat as invalid if all 0
        ],
        schema="""count integer, 1_age integer, 2_age integer, 3_age integer""",
    )

    output_df = assign_household_under_2_count(input_df, "count", r"[1-3]_age")
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
