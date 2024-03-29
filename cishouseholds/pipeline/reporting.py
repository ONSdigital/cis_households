from io import BytesIO
from typing import Dict

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from cishouseholds.edit import update_column_values_from_map
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import get_run_id
from cishouseholds.pyspark_utils import get_or_create_spark_session


def generate_lab_report(df: DataFrame) -> DataFrame:
    """
    Generate lab report of latest 7 days of results
    """
    current_date = F.lit(df.orderBy(F.desc("file_date")).head().file_date)
    df = df.filter(F.date_sub(current_date, 7) < F.col("survey_completed_datetime"))
    swab_df = df.select("swab_sample_barcode", "swab_taken_datetime", "survey_completed_datetime").filter(
        ~(
            ((F.col("swab_taken_datetime").isNull()) & (F.col("survey_completed_datetime").isNull()))
            | F.col("swab_sample_barcode").isNull()
        )
    )
    blood_df = df.select("blood_sample_barcode", "blood_taken_datetime", "survey_completed_datetime").filter(
        ~(
            ((F.col("blood_taken_datetime").isNull()) & (F.col("survey_completed_datetime").isNull()))
            | F.col("blood_sample_barcode").isNull()
        )
    )
    return swab_df, blood_df


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
    Returns a dataframe containing participants reported to have been visited multiple times in 1 day.

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


def generate_error_table(table_name: str, error_priority_map: dict) -> DataFrame:
    """
    Generates tables of errors and their respective counts present in
    current and previous pipeline run ordered by a custome priorty ranking
    set in pipeline config.

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


def count_variable_option(df: DataFrame, column_inv: str, column_value: str):
    """
    Counts occurence of a specific value in a column

    Parameters
    --------
    df
        Dataframe
    column_inv
        column you want the value count from
    column_value
        value you want counting in the specified column

    """

    df_filt = df.withColumn("col_value", F.when(F.col(column_inv) == column_value, F.lit(1)).otherwise(0))
    sum_value = df.count() - df_filt.filter(df_filt["col_value"] == 0).count()

    count_data = [
        # fmt:off
        (column_inv, column_value, sum_value)
        # fmt:on
    ]

    schema = """
            column_name string,
            column_value string,
            count integer
            """

    output_df = get_or_create_spark_session().createDataFrame(data=count_data, schema=schema)
    return output_df
