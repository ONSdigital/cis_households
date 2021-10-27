from chispa import assert_df_equality

from cishouseholds.merge import join_dataframes


def test_join_dataframes(spark_session):

    input_df1 = spark_session.createDataFrame(
        data=[
            ("1", 1),
            ("2", 2),
            ("2", 3),
            ("3", 4),
            ("4", 5),
            ("4", 6),
        ],
        schema="id string, unique_id_1 integer",
    )
    input_df2 = spark_session.createDataFrame(
        data=[
            ("1", 7),
            ("2", 8),
            ("3", 9),
            ("3", 10),
            ("4", 11),
            ("4", 12),
        ],
        schema="id string, unique_id_2 integer",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("1", 1, 7),
            ("2", 2, 8),
            ("2", 3, 8),
            ("3", 4, 9),
            ("3", 4, 10),
            ("4", 5, 11),
            ("4", 5, 12),
            ("4", 6, 11),
            ("4", 6, 12),
        ],
        schema="id string, unique_id_1 integer, unique_id_2 integer",
    )

    output_df = join_dataframes(input_df1, input_df2, join_type="outer", on="id")
    ordering_columns = ["id", "unique_id_1", "unique_id_2"]
    assert_df_equality(
        output_df.orderBy(*ordering_columns), expected_df.orderBy(*ordering_columns), ignore_nullable=True
    )
