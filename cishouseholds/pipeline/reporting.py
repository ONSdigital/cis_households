from io import BytesIO
from typing import Dict
from typing import List

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from cishouseholds.edit import update_column_values_from_map
from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import get_run_id


def dfs_to_bytes_excel(sheet_df_map: Dict[str, DataFrame]) -> BytesIO:
    """
    Convert a dictionary of Spark DataFrames into an Excel Object.

    Parameters
    ----------
    sheet_df_map
        A dictionary of Spark DataFrames - keys in this dictionary become
        names of the sheets in Excel & the values in this dictionary become
        the content in the respective sheets
    """
    output = BytesIO()
    with pd.ExcelWriter(output) as writer:
        for sheet, df in sheet_df_map.items():
            df.toPandas().to_excel(writer, sheet_name=sheet, index=False)
    return output


def multiple_visit_1_day(df: DataFrame, participant_id: str, visit_id: str, date_column: str, datetime_column: str):
    """
    Tracks multiple visits by a participant

    Parameters
    ----------
    df
        The input dataframe to process
    participant_id
        The column name containing participant ids
    visit_id
        The column name containing visit ids
    date_column
        The column name containing visit date
    datetime_column
        The column name containing visit datetime
    """
    window = Window.partitionBy(participant_id, date_column)  # .orderBy(date_column, datetime_column)

    df = df.withColumn("FLAG", F.count(visit_id).over(window))
    df_multiple_visit = df.filter(F.col("FLAG") > 1)  # get only multiple visit
    df_multiple_visit = df_multiple_visit.withColumn(
        "FLAG", F.rank().over(window.orderBy(date_column, F.desc(datetime_column)))
    )
    df_multiple_visit = df_multiple_visit.filter(F.col("FLAG") == 1)
    return df_multiple_visit.drop("FLAG")


def unmatching_antibody_to_swab_viceversa(
    swab_df: DataFrame, antibody_df: DataFrame, column_list: List[str]
) -> DataFrame:
    """
    Identifies participants who are present in Swab dataframe but not in Antibody
    dataframe and vice versa.

    Parameters
    ----------
    swab_df
        The Swabs dataframe
    antibody_df
        The Antibody dataframe
    column_list
        A list of columns to return in the resulting dataframe
    """
    unmatched_swab_df = swab_df.join(antibody_df, on="barcode", how="left_anti").select(*column_list)
    unmatched_antibody_df = antibody_df.join(swab_df, on="barcode", how="left_anti").select(*column_list)

    df = union_multiple_tables(tables=[unmatched_swab_df, unmatched_antibody_df])
    return df


def generate_error_table(table_name: str, error_priority_map: dict) -> DataFrame:
    """
    Generates error tables

    Parameters
    ----------
    table_name
        Name of a hdfs table of survey responses passing/failing validation checks
    error_priority_map
        Error priority dictionary
    """
    df = extract_from_table(table_name)
    df_new = df.filter(F.col("run_id") == get_run_id()).groupBy("validation_check_failures").count()
    df_previous = df.filter(F.col("run_id") == (get_run_id() - 1)).groupBy("validation_check_failures").count()
    df = (
        df_previous.withColumnRenamed("count", "count_previous")
        .withColumnRenamed("run_id", "run_id_previous")
        .join(
            df_new.withColumnRenamed("count", "count_current").withColumnRenamed("run_id", "run_id_current"),
            on="validation_check_failures",
            how="fullouter",
        )
    )
    df = df.withColumn("ORDER", F.col("validation_check_failures"))
    df = update_column_values_from_map(df, "ORDER", error_priority_map, default_value=9999)
    return df.orderBy("ORDER").drop("ORDER")
