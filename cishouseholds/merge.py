from typing import Any
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.pipeline.load import update_table
from cishouseholds.pyspark_utils import get_or_create_spark_session


def prepare_for_union(tables: List[DataFrame]):
    spark_session = get_or_create_spark_session()
    dataframes = []

    if len(tables) == 0:
        raise ValueError("Tried to execute union with no tables to union")  # functional

    for table in tables:
        if type(table) == str:
            dataframes.append(spark_session.read.parquet(table))
        else:
            dataframes.append(table)

    all_columns, missing = get_missing_columns(dataframes)
    dataframes = add_missing_columns(dataframes, missing)

    dataframes = [df.select(*all_columns) for df in dataframes]

    return dataframes


def add_after(list: list, element: Any, add: Any):
    """
    Add {add} after {element} in {list}
    """
    el_index = list.index(element)
    new_list = list[: el_index + 1]
    new_list.append(add)
    new_list += list[el_index + 1 :]  # noqa: E203
    return new_list


def get_missing_columns(dataframes: List[DataFrame]):
    """
    get missing columns from each dataframe compared across all input dfs
    """
    all_columns: List[Any] = []

    for df in dataframes:
        i = 0
        for col, type in df.dtypes:
            if col not in all_columns:
                if i > 0:
                    all_columns = add_after(all_columns, prev_element, {"col": col, "type": type})  # noqa: F821
                else:
                    all_columns.append({"col": col, "type": type})
            prev_element: dict = {"col": col, "type": type}  # noqa: F841
            i += 1

    missing = [[col for col in all_columns if col["col"] not in df.columns] for df in dataframes]
    col_strings = []
    for col in all_columns:
        if col["col"] not in col_strings:
            col_strings.append(col["col"])
    return col_strings, missing


def add_missing_columns(dataframes: List[DataFrame], missing: List[List[Any]]):
    """
    add columns with None casted to the correct data type and name that are missing
    from the respective dataframe
    """
    transformed_dfs = []
    for df, missing_set in zip(dataframes, missing):
        for col in missing_set:
            df = df.withColumn(col["col"], F.lit(None).cast(col["type"]))
        transformed_dfs.append(df)
    return transformed_dfs


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


def left_join_keep_only_non_null_right(left_df: DataFrame, right_df: DataFrame, join_on_columns: list = []):
    """
    Performs a left join on 2 dataframes, keeping the values from right side only if they are not null,
    otherwise it keeps the values from the left
    """
    right_cols = [col for col in right_df.columns if col not in join_on_columns]
    for col in right_cols:
        right_df = right_df.withColumnRenamed(col, col + "_right")

    df = left_df.join(right_df, on=join_on_columns, how="left")
    for col in right_cols:
        if col in df.columns and ((col + "_right") in df.columns):
            df = df.withColumn(col, F.coalesce(col + "_right", col))
        df = df.drop(col + "_right")
    return df


def left_join_keep_right(left_df: DataFrame, right_df: DataFrame, join_on_columns: list = []):
    """
    Performs a left join on 2 dataframes and removes the additional in the right dataframe from the left
    """
    left_df = left_df.select(*[col for col in left_df.columns if col not in right_df.columns or col in join_on_columns])
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
