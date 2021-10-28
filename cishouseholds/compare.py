from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import copy

from cishouseholds.pyspark_utils import get_or_create_spark_session

spark = get_or_create_spark_session()


def prepare_for_union(df1: Union[str, DataFrame], df2: Union[str, DataFrame]):
    spark = get_or_create_spark_session()
    if isinstance(df1, str):
        df1 = spark.read.parquet(df1)
    if isinstance(df2, str):
        df2 = spark.read.parquet(df2)
    copy_df1 = df1
    copy_df2 = df2
    missmatches_df, missmatches_ref = get_inconsistent_columns(df1, df2)
    for col in missmatches_ref:
        df1 = add_matching_col(df1, df2, col)
    for col in missmatches_df:
        df2 = add_matching_col(df2, df1, col)
    df1 = df1.select([x.name for x in df2.schema.fields])
    col_order = get_new_order(
        [x.name for x in copy_df2.schema.fields], [x.name for x in copy_df1.schema.fields], missmatches_df
    )
    df2 = df2.select([col for col in col_order])
    df1 = df1.select([col for col in col_order])
    return df1, df2


def add_after(list: list, element: str, add: str):
    el_index = list.index(element)
    new_list = list[: el_index + 1]
    new_list.append(add)
    new_list += list[el_index + 1 :]  # noqa: E203
    return new_list


def get_new_order(df1_names: list, df2_names: list, df2_missmatches: list):
    right_of = {}
    for col in df2_missmatches:
        right_of[df2_names[df2_names.index(col) - 1]] = col
    while len(right_of) > 0:
        added = []
        for key, val in right_of.items():
            if key in df1_names:
                df1_names = add_after(df1_names, key, val)
                added.append(key)
        for key in added:
            right_of.pop(key)
    return df1_names


def get_inconsistent_columns(df1: DataFrame, df2: DataFrame):
    df1_schema = df1.schema.fields
    df2_schema = df2.schema.fields
    df1_names = [x.name for x in df1_schema]
    df2_names = [x.name for x in df2_schema]
    matches = set(df1_names) & set(df2_names)
    df_1_missmatches = set(df1_names) ^ matches
    df_2_missmatches = set(df2_names) ^ matches
    return list(df_1_missmatches), list(df_2_missmatches)


def add_matching_col(df: DataFrame, df_ref: DataFrame, col_name: str):
    col_name, type = df_ref.select(col_name).dtypes[0]
    return df.withColumn(col_name, F.lit(None).cast(type))
