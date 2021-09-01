from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def assign_count_of_occurrences_column(df: DataFrame, reference_column: str, column_name_to_assign: str):
    """
    Derive column to count the number of occurrences of the value in the reference_column over the entire dataframe.

    Parameters
    ----------
    df
    reference_column
        Name of column to count value occurrences
    column_name_to_assign
        Name of column to be created
    """

    window = Window.partitionBy(reference_column)

    return df.withColumn(column_name_to_assign, F.count(reference_column).over(window).cast("integer"))


def assign_absolute_offset(df: DataFrame, column_name_to_assign: str, reference_column: str, offset: float):
    """
    Assign column based on the absolute value of an offsetted number.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be created
    reference_column
        Name of column to calculate values for new column from
    offset
        Amount to offset each reference_column value by

    Notes
    -----
    Offset will be subtracted.
    """
    return df.withColumn(column_name_to_assign, F.abs(F.col(reference_column) - offset))


def assign_unique_identifier_column(df: DataFrame, column_name_to_assign: str, ordering_columns: list):
    """
    Derive column with unique identifier for each record.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be created
    ordering_columns
        Columns to define order of records to assign an integer value from 1 onwards
        This order is mostly for comparison/proving purposes with stata output
    """

    window = Window.orderBy(*ordering_columns)
    return df.withColumn(column_name_to_assign, F.row_number().over(window))


def join_dataframes(df1: DataFrame, df2: DataFrame, reference_column: str, join_type: str = "outer"):
    """
    Join two datasets.

    Parameters
    ----------
    df1
    df2
    reference_column
        Column for join to occur on
    join_type
        Specify join type to apply to .join() method
    """
    return df1.join(df2, on=reference_column, how=join_type)
