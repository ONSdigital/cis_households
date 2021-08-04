from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def filter_all_not_null(df: DataFrame, reference_columns: List[str]) -> DataFrame:
    """
    Filter rows which have NULL values in all the specified columns.
    From households_aggregate_processes.xlsx, filter number 2.

    Parameters
    ----------
    df
    reference_columns
        Columns to check for missing values in, all
        must be missing for the record to be dropped.
    """
    return df.na.drop(how="all", subset=reference_columns)


def filter_duplicates_by_time_and_threshold(
    df: DataFrame,
    first_reference_column: str,
    second_reference_column: str,
    third_reference_column: str,
    fourth_reference_column: str,
    time_threshold: float,
) -> DataFrame:
    """
    Write a function filters to remove records with a duplicate in V1 and V2 (together),
    to keep the earliest given the timestamp V3. **STEP ONE**
    The filter should only drop records when the difference in V3 between the earliest V3
    and other records is < 1.5h, **STEP 2**
    and the difference in V4 between the earliest V3 and other records is < 1e-5

    TODO
    Parameterize - float thresh
    Add in later conditionss
    """

    window = Window.partitionBy(first_reference_column, second_reference_column).orderBy(third_reference_column)
    df = (
        df.withColumn("duplicate_id", F.row_number().over(window))
        .withColumn("first", (F.first(third_reference_column).over(window).cast("long")) / (60 * 60))
        .withColumn("second", (F.col(third_reference_column).cast("long")) / (60 * 60))
        .withColumn("result", F.abs(F.col("first") - F.col("second")))
        .withColumn("within_time_threshold", F.col("result") < time_threshold)
    )
    # ripe for refactoring

    print(df.show())

    df = df.filter((F.col("duplicate_id") == 1) | ~F.col("within_time_threshold"))
    df = df.drop("duplicate_id", "first", "second", "result", "within_time_threshold")
    return df
