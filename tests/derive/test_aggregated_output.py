import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.derive import aggregated_output_groupby
from cishouseholds.derive import aggregated_output_window


def test_aggregated_output(spark_session):

    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # id    sub_id   col_1     col_2     col_3   col_1_max      col_2_min
            (1,     1,       1,        1,        1,      1,             1),
            (2,     1,       7,        7,        7,      8,             7),
            (2,     1,       8,        7,        7,      8,             7),
            # group id 3, 2 subgroups 2, and 3.
            (3,     2,       0,        0,        1,      6,             0),
            (3,     2,       6,        1,        6,      6,             0),
            (3,     3,       6,        1,        6,      6,             0),
            # fmt: on
        ],
        schema="""
            id integer,
            sub_id integer,
            col_1 integer,
            col_2 integer,
            col_3 integer,
            col_1_max integer,
            col_2_min integer
        """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # id        col_1_max   col_2_min
            (1,         1,          1),
            (2,         8,          7),
            (3,         6,          0),
            # fmt: on
        ],
        schema="""
            id integer,
            col_1_max integer,
            col_2_min integer
        """,
    )
    # aggregated groupby
    output_df_gr = aggregated_output_groupby(
        df=input_df,
        column_group="id",
        column_name_list=["col_1", "col_2"],
        apply_function_list=["max", "min"],
        column_name_to_assign_list=["col_1_max", "col_2_min"],
    )
    assert_df_equality(expected_df, output_df_gr, ignore_column_order=True, ignore_row_order=True)

    # aggregated window
    input_df_w = input_df.drop("col_1_max", "col_2_min")

    output_df_w = aggregated_output_window(
        df=input_df_w,
        column_window_list=["id"],
        column_name_list=["col_1", "col_2"],
        apply_function_list=["max", "min"],
        column_name_to_assign_list=["col_1_max", "col_2_min"],
    )
    assert_df_equality(input_df, output_df_w, ignore_column_order=True, ignore_row_order=True)
