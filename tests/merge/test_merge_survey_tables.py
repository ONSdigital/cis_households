from chispa import assert_df_equality

from cishouseholds.merge import merge_survey_tables


def test_merge_survey_tables(spark_session):
    input_df0 = spark_session.createDataFrame(
        data=[
            ("a1", 1, 2),
            ("a2", 2, 4),
            ("a2", 3, 6),
        ],
        schema="id string, col1 integer, col3 integer",
    )
    input_df1 = spark_session.createDataFrame(
        data=[
            ("b1", 1, 9),
            ("b2", 1, 9),
            ("b3", 1, 9),
        ],
        schema="id string, col1 integer, col2 integer",
    )
    input_df2 = spark_session.createDataFrame(
        data=[
            ("c1", 1, 27, 26),
            ("c2", 2, 26, 25),
            ("b2", 3, 25, 24),
        ],
        schema="id string, col1 integer, col2 integer, col3 integer",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("a1", 1, None, 26),
            ("a2", 2, None, 25),
            ("a2", 3, None, 24),
            ("b1", 1, 9, None),
            ("b2", 1, 9, None),
            ("b3", 1, 9, None),
            ("c1", 1, 27, 26),
            ("c2", 2, 26, 25),
            ("b2", 3, 25, 24),
        ],
        schema="id string, col1 integer, col2 integer, col3 integer",
    )
    output_df = merge_survey_tables([input_df0, input_df1, input_df2])
    assert_df_equality(expected_df, output_df)
