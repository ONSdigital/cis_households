from chispa import assert_df_equality

from cishouseholds.edit import update_column_if_ref_in_list


def test_update_column_if_ref_in_list(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (2, 1),
            (None, 2),
            (None, None),
        ],
        schema="""col integer, ref integer""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (2, 1),
            (99, 2),
            (None, None),
        ],
        schema="""col integer, ref integer""",
    )

    output_df = update_column_if_ref_in_list(input_df, "col", None, 99, "ref", [1, 2])
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
