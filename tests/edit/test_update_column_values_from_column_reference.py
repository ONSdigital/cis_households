from chispa import assert_df_equality

from cishouseholds.edit import update_column_values_from_map


def test_update_column_values_from_column_reference(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (2, 1),
            (11, 2),
            (None, 3),
        ],
        schema="""col integer, ref integer""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (99, 1),
            (98, 2),
            (None, 3),
        ],
        schema="""col integer, ref integer""",
    )

    output_df = update_column_values_from_map(df=input_df, column="col", condition_column="ref", map={1: 99, 2: 98})
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
