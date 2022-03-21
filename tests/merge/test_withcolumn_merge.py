from chispa import assert_df_equality
from pyspark.sql.window import Window

from cishouseholds.merge import assign_group_and_row_number_columns


def test_withColumn_assign_group_and_row_number_columns(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1,     'a',        'd'),
            (2,     'b',        'e'),
            (3,     'c',        'f'),
            # fnt: on
        ],
        schema="column_1 integer, column_2 string, column_3 string",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1,     'a',        'd',        1,      1),
            (2,     'b',        'e',        1,      2),
            (3,     'c',        'f',        1,      3),
            # fnt: on
        ],
        schema="column_1 integer, column_2 string, column_3 string, count integer, column_2 integer",
    )
    window = Window.partitionBy("column_1").orderBy("column_1")
    output_df = assign_group_and_row_number_columns(
        df=input_df,
        window=window,
        row_num_column="column_1",
        group_column="column_2",
        group_by_column="column_3",
    )
    # assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
