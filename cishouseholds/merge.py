from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.pipeline.load import update_table
from cishouseholds.prepare_for_union import prepare_for_union


def union_dataframes_to_hive(output_table_name: str, dataframe_list: List[DataFrame]) -> None:
    """
    Sequentially append a list of dataframes to a new HIVE table. Overwrites if table exists.
    Columns are made consistent between dataframes, filling Nulls where columns were not present on all dataframes.
    """
    dataframes = prepare_for_union(tables=dataframe_list)

    update_table(dataframes[0], output_table_name, write_mode="overwrite")
    for df in dataframes[1:]:
        update_table(df, output_table_name, write_mode="append")


def union_multiple_tables(tables: List[DataFrame]) -> DataFrame:
    """
    Given a list of tables combine them through a union process
    and create null columns for columns inconsistent between all tables
    Parameters
    ----------
    tables
        list of objects representing the respective input tables
    """
    dataframes = prepare_for_union(tables=tables)
    merged_df = dataframes[0]
    for dfn in dataframes[1:]:
        merged_df = merged_df.union(dfn)
    return merged_df


def left_join_keep_right(left_df: DataFrame, right_df: DataFrame, join_on_columns: list = []):
    """
    Performs a left join on 2 dataframes and removes the additional in the right dataframe from the left
    """
    left_df = left_df.select(
        *[col for col in left_df.columns if col not in right_df.columns and col not in join_on_columns]
    )
    return left_df.join(right_df, on=join_on_columns, how="left")


def null_safe_join(
    left_df: DataFrame, right_df: DataFrame, null_safe_on: list = [], null_unsafe_on: list = [], how="left"
):
    """
    Performs a join on equal columns, where a subset of the join columns can be null safe.

    Parameters
    ----------
    left_df
    right_df
    null_safe_on
        columns to make a null safe equals comparison on
    null_unsafe_on
        columns to make a null unsafe equals comparison on
    how
        join type
    """
    for column in null_safe_on + null_unsafe_on:
        right_df = right_df.withColumnRenamed(column, column + "_right")

    null_unsafe_conditions = [f"({column} == {column + '_right'})" for column in null_unsafe_on]
    null_safe_conditions = [f"({column} <=> {column + '_right'})" for column in null_safe_on]

    join_condition = " AND ".join(null_unsafe_conditions + null_safe_conditions)

    joined_df = left_df.join(right_df, on=F.expr(join_condition), how=how)

    for column in null_safe_on + null_unsafe_on:
        # Recover right entries from outer/right joins
        joined_df = joined_df.withColumn(column, F.coalesce(F.col(column), F.column(column + "_right")))

    return joined_df.drop(*[column + "_right" for column in null_safe_on + null_unsafe_on])
