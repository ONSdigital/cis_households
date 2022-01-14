from typing import Any
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.pyspark_utils import get_or_create_spark_session


def prepare_for_union(tables: List[DataFrame]):
    spark_session = get_or_create_spark_session()
    dataframes = []

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
