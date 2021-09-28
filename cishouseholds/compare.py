from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.pyspark_utils import get_or_create_spark_session

spark = get_or_create_spark_session()


def prepare_for_union(df: DataFrame, reference: Union[str, DataFrame], rearrange_ref: bool = False):
    df.show()
    if type(reference) == str:
        df_ref = spark.read.parquet("/temp/out/people.parquet")
    else:
        df_ref = reference
    df_ref.show()
    copy_df = df
    copy_ref = df_ref
    missmatches_df, missmatches_ref = get_inconsistent_columns(df, df_ref)
    for col in missmatches_ref:
        df = add_matching_col(df, df_ref, col)
    for col in missmatches_df:
        df_ref = add_matching_col(df_ref, df, col)
    df = df.select([x.name for x in df_ref.schema.fields])
    col_order = get_new_order(
        [x.name for x in copy_ref.schema.fields], [x.name for x in copy_df.schema.fields], missmatches_df
    )
    df_ref = df_ref.select([col for col in col_order])
    df = df.select([col for col in col_order])
    df.show()
    df_ref.show()
    return df, df_ref


def add_after(list: list, element: str, add: str):
    el_index = list.index(element)
    new_list = list[: el_index + 1]
    new_list.append(add)
    try:
        new_list += list[el_index + 1 :]  # noqa: E203
    except Exception as e:
        print(e)
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


# comments
