from chispa.dataframe_comparer import assert_df_equality

from cishouseholds.pipeline.post_merge_processing import transpose_df_with_string


def test_transpose_df_with_string(spark_session):
    # wide to long
    input_df_wide = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('he.llo', 1, 78.9, 'bye'),
                ('hola', 2, 31.9, 'adios'),
            # fmt: on
        ],
        schema="""
        string_col string, integer_col integer, float_col double, string2_col string
        """,
    )

    output_df_long = transpose_df_with_string(input_df_wide)

    expected_df_long = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('string_col',      'he.llo',  'hola'),
                ('integer_col',     '1',       '2'),
                ('float_col',       '78.9',    '31.9'),
                ('string2_col',     'bye',     'adios'),
            # fmt: on
        ],
        schema="""column_name string, row_1 string, row_2 string""",
    )
    assert_df_equality(
        expected_df_long, output_df_long, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True
    )

    # long to wide
    output_df_wide = transpose_df_with_string(expected_df_long, type_transpose="wide")
    assert_df_equality(
        output_df_wide, input_df_wide, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True
    )
