from typing import List

from pyspark.sql import DataFrame


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
