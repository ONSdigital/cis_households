from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def filter_before_date_or_null(df: DataFrame, date_column: str, min_date: str):
    """
    Filter rows which have a date before a given `min_date`.

    Parameters
    ----------
    df
    date_column
        a column containing a formatted date
    min_date
        a  minimum date within the 'date_column' for which to retain rows
    """
    return df.filter((F.col(date_column) >= min_date) | (F.col(date_column).isNull()))


def filter_all_not_null(df: DataFrame, reference_columns: List[str]) -> DataFrame:
    """
    Filter rows which have NULL values in all the specified columns.
    From households_aggregate_processes.xlsx, filter number 2.

    Parameters
    ----------
    df
    reference_columns
        Columns to check for missing values in, all must be missing for the record to be
        dropped. If you want all columns in the input dataframe to be considered, then simply
        pass `{{df}}.columns` as input to this argument
    """
    return df.na.drop(how="all", subset=reference_columns)


def filter_by_cq_diff(
    df: DataFrame, comparing_column: str, ordering_column: str, tolerance: float = 0.00001
) -> DataFrame:
    """
    This function works out what columns have a float value difference less than 10-^5 or 0.00001
        (or any other tolerance value inputed) given all the other columns are the same and
        considers it to be the same dropping or deleting the repeated values and only keeping one entry.

    Parameters
    ----------
    df
    comparing_column
    ordering_column
    tolerance
    """
    column_list = df.columns
    column_list.remove(comparing_column)

    windowSpec = Window.partitionBy(column_list).orderBy(ordering_column)
    df = df.withColumn("first_value_in_duplicates", F.first(comparing_column).over(windowSpec))
    df = df.withColumn(
        "duplicates_first_record", F.abs(F.col("first_value_in_duplicates") - F.col(comparing_column)) < tolerance
    )

    difference_window = Window.partitionBy(column_list + ["duplicates_first_record"]).orderBy(ordering_column)
    df = df.withColumn("duplicate_number", F.row_number().over(difference_window))

    df = df.filter(~(F.col("duplicates_first_record") & (F.col("duplicate_number") != 1)))
    df = df.drop("first_value_in_duplicates", "duplicates_first_record", "duplicate_number")

    return df


def assign_date_interval_and_flag(
    df: DataFrame,
    column_name_inside_interval: str,
    column_name_time_interval: str,
    start_datetime_reference_column: str,
    end_datetime_reference_column: str,
    lower_interval: Union[int, float],
    upper_interval: Union[int, float],
    interval_format: str = "hours",
) -> DataFrame:
    """
    This function gives the time interval in either hours (by default) or days
    in a column by given two date columns and says whether it is inside and
    upper and lower interval. If the difference of dates is within the upper and
    lower time intervals, the function will output None and an integer 1 if the
    difference in dates are outside of those intervals.

    Parameters
    ----------
    df
    column_name_inside_interval
        Name of the column that returns whether the difference in dates are
        within the upper/lower limits if within, it will return None, if outside
        will return an integer 1.
    column_name_time_interval
        Name of the column that returns the difference between start and end
        date and adds at the end of the column name whether it is in hours or
        days
    start_datetime_reference_column
        Earliest date in string format yyyy-mm-dd hh:mm:ss.
    end_datetime_reference_column
        Latest date in string format yyyy-mm-dd hh:mm:ss.
    lower_interval
        Marks how much NEGATIVE time difference can have between
        end_datetime_reference_column and start_datetime_reference_column.
        Meaning how the end_datetime_reference_column can be earlier than
        start_datetime_reference_column
    upper_interval
        Marks how much POSITIVE time difference can have between
        end_datetime_reference_column and start_datetime_reference_column
    interval_format
        By default will be a string called 'hours' if upper and lower
        intervals are input as days, define interval_format to 'days'.
        These are the only two possible formats.

    Notes
    -----
    Lower_interval should be a negative value if start_datetime_reference_column
    is after end_datetime_reference_column."""

    # by default, Hours but if days, apply change factor
    if interval_format == "hours":  # to convert hours to seconds
        conversion_factor = 3600  # 1h has 60s*60min seconds = 3600 seconds
    elif interval_format == "days":
        conversion_factor = 86400  # 1 day has 60s*60min*24h seconds = 86400 seconds

    column_name_time_interval = column_name_time_interval + "_" + interval_format

    # FORMULA: (end_datetime_reference_column - start_datetime_reference_column) in
    # seconds/conversion_factor in seconds
    df = df.withColumn(
        column_name_time_interval,
        (
            F.to_timestamp(F.col(end_datetime_reference_column)).cast("long")
            - F.to_timestamp(F.col(start_datetime_reference_column)).cast("long")
        )
        / conversion_factor,  # 1 day has 60s*60min*24h seconds = 86400 seconds
    )

    return df.withColumn(
        column_name_inside_interval,
        F.when(~F.col(column_name_time_interval).between(lower_interval, upper_interval), 1).otherwise(None),
    )


def filter_exclude_source_file(df: DataFrame, source_file_col: str, files_to_exclude: list):
    """
    Function to exclude specific files from pipeline processing

    Parameters
    --------
    df
    source_file_column = Column in input dataframe which contains the source file
    files_to_exclude = List of files to exclude (feed in from config)
    """
    for item in files_to_exclude:
        df = df.filter(~F.col(source_file_col).isin(item))

    return df


def filter_exclude_by_pattern(df: DataFrame, column: str, pattern: str) -> DataFrame:
    """
    Function to filter dataframe by excluding pattern in specific column

    Parameters
    --------
    df
    column
        String name of column in dataframe to filter on.
    pattern
        String or raw string literal to match and remove from dataframe
    """
    return df.filter(~F.col(column).rlike(pattern))
