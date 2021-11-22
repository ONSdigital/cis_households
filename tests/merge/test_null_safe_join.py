from chispa.dataframe_comparer import assert_df_equality

from cishouseholds.merge import null_safe_join


def test_null_safe_join(spark_session):
    # fmt: off
    left_df = spark_session.createDataFrame(
        data=[
            ("1", "A",  "L"),
            ("3", "A",  "L"),
            ("4", None, "L"),
        ],
        schema="""num string, letter string, side_l string""",
    )
    right_df = spark_session.createDataFrame(
        data=[
            ("2", "A",  "R"),
            ("3", "A",  "R"),
            ("4", None, "R"),
        ],
        schema="""num string, letter string, side_r string""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("1", "A",  "L",  None),
            ("2", "A",  None, "R" ),
            ("3", "A",  "L",  "R" ),
            ("4", None, "L",  "R" ),
        ],
        schema="""num string, letter string, side_l string, side_r string""",
    )
    # fmt: on

    output_df = null_safe_join(left_df, right_df, null_safe_on=["letter"], null_unsafe_on=["num"], how="outer")

    assert_df_equality(expected_df, output_df, ignore_row_order=True, ignore_column_order=True)
