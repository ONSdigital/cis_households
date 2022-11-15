import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import get_differences


def test_get_differences(spark_session):
    input_df_base = spark_session.createDataFrame(
        data=[
            (1, "A", 1, True),
            (2, "B", 2, True),
            (3, "C", 3, True),
        ],
        schema="id integer, string string, int integer, bool boolean",
    )
    input_df_compare = spark_session.createDataFrame(
        data=[
            (1, "A", 1, True),
            (2, "X", 99, True),
            (3, "C", 69420, None),
        ],
        schema="id integer, string string, int integer, bool boolean",
    )
    counts_df = spark_session.createDataFrame(
        data=[
            ("string", 1),
            ("int", 2),
            ("bool", 1),
        ],
        schema="column_name string, difference_count integer",
    )
    diffs_df = spark_session.createDataFrame(
        data=[
            (2, "string"),
            (2, "int"),
            (3, "int"),
            (3, "bool"),
        ],
        schema="id integer, column_name string",
    )
    output_df_counts, output_df_diffs = get_differences(input_df_base, input_df_compare, "id")

    assert_df_equality(counts_df, output_df_counts, ignore_nullable=True)
    assert_df_equality(diffs_df, output_df_diffs, ignore_nullable=True)
