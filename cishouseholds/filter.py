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


def filter_by_cq_diff(
                        df: DataFrame, 
                        comparing_column: str, 
                        ordering_column: str, 
                        tolerance: float = 0.00001) -> DataFrame:
    """
    This function works out what columns have a float value difference less than 10-^5 or 0.00001
        (or any other tolerance value inputed) given all the other columns are the same and
        considers it to be the same dropping or deleting the repeated values and only keeping one entry.

    Parameters
    ----------
    df:
        Pyspark DataFrame that will be computed
    comparing_column:
        the float value column where the differences of 10^-5 will be computed.
    ordering_column:
        just for output ascetics, put the name of the column to be ordered by,
        recommended to be date.
    tolerance:
        for the ticket 753 F3 the value used is 10^-5 as standard but any other
            tolerance can be applied.
    Return
    ------
    df: pyspark.sql.dataframe
    """
    column_list = df.columns
    column_list.remove(comparing_column)

    windowSpec = Window.partitionBy(column_list).orderBy(ordering_column)
    df = df.withColumn("first_value_in_duplicates", 
                        F.first(comparing_column).over(windowSpec))
    df = df.withColumn("duplicates_first_record", 
                        F.abs(F.col("first_value_in_duplicates") - F.col(comparing_column)) < tolerance)

    difference_window = Window.partitionBy(column_list + ["duplicates_first_record"]).orderBy(ordering_column)
    df = df.withColumn("duplicate_number", 
                        F.row_number().over(difference_window))

    df = df.filter(~(F.col("duplicates_first_record") & (F.col("duplicate_number") != 1)))
    df = df.drop("first_value_in_duplicates", "duplicates_first_record", "duplicate_number")

    return df(ordering_column, comparing_column)
