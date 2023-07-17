import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.reporting import generate_comparison_tables


def test_generate_comparison_tables(spark_session):
    input_df_base = spark_session.createDataFrame(
        data=[
            (1, "A", 1, True),
            (2, "B", 2, True),
            (3, "C", 3, True),
            (4, "D", 4, None),
        ],
        schema="id integer, string string, int integer, bool boolean",
    )
    input_df_compare = spark_session.createDataFrame(
        data=[
            (1, "A", 1, True),
            (2, "X", 99, True),
            (3, "C", 69420, None),
            (4, "D", 4, True),
        ],
        schema="id integer, string string, int integer, bool boolean",
    )
    counts_df = spark_session.createDataFrame(
        data=[
            ("string", 1, 1),
            ("int", 2, 2),
            ("bool", 2, 1),
        ],
        schema="column_name string, difference_count integer, difference_count_non_improved integer",
    )
    diffs_df = spark_session.createDataFrame(
        data=[(2, "string"), (2, "int"), (3, "int"), (3, "bool"), (4, "bool")],
        schema="id integer, column_name string",
    )
    output_df_counts, output_df_diffs = generate_comparison_tables(input_df_base, input_df_compare, "id")
    assert_df_equality(
        counts_df, output_df_counts, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True
    )
    assert_df_equality(diffs_df, output_df_diffs, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
