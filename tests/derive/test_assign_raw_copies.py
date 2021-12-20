from chispa import assert_df_equality

from cishouseholds.derive import assign_raw_copies


def test_assign_raw_copies(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "A", 1, "A"),
            (None, None, None, None),
        ],
        schema="""int integer, str string, int_raw integer, str_raw string""",
    )
    output_df = assign_raw_copies(expected_df.drop("int_raw", "str_raw"), ["int", "str"])
    assert_df_equality(expected_df, output_df, ignore_row_order=True)
