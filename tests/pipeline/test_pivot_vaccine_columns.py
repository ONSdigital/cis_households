from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import pivot_vaccine_columns


def test_pivot_vaccine_columns(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, 1, 4, 2, 3),
            (2, 2, 8, 4, 6),
            (3, 3, 12, 6, 9),
            (4, 4, 16, 8, 12),
            (5, None, None, None, None),
        ],
        schema="id integer, a_1 integer, a_2 integer, b_1 integer, b_2 integer",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 1, 1, 2),
            (1, 2, 4, 3),
            (2, 1, 2, 4),
            (2, 2, 8, 6),
            (3, 1, 3, 6),
            (3, 2, 12, 9),
            (4, 1, 4, 8),
            (4, 2, 16, 12),
            (5, 1, None, None),
            (5, 2, None, None),
        ],
        schema="id integer, row integer, a integer, b integer",
    )
    output_df = pivot_vaccine_columns(df=input_df, row_number_column="row", prefixes=["a", "b"])
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
