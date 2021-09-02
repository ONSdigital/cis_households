from typing import List
from typing import Union

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
    time_threshold: float = 1.5,
    float_threshold: float = 0.00001,
) -> DataFrame:
    """
    Drop duplicates based on two identitical column values if third and fourth column and not both within
    a threshold difference from the first duplicate record.
    From households_aggregate_processes.xlsx, filter number 4.
    Parameters
    ----------
    df
    first_reference_column
        First column with duplicate value
    second_reference_column
        Second column with duplicate value
    third_reference_column
        Column used for time based threshold difference, timestamp
    fourth_reference_column
        Column used for numeric based threshold difference, float
    """

    window = Window.partitionBy(first_reference_column, second_reference_column).orderBy(third_reference_column)

    df = df.withColumn("duplicate_id", F.row_number().over(window))

    df = df.withColumn(
        "within_time_threshold",
        (
            F.abs(
                F.first(third_reference_column).over(window).cast("long") - F.col(third_reference_column).cast("long")
            )
            / (60 * 60)
        )
        < time_threshold,
    )

    df = df.withColumn(
        "within_float_threshold",
        F.abs(F.first(fourth_reference_column).over(window) - F.col(fourth_reference_column)) < float_threshold,
    )

    df = df.filter((F.col("duplicate_id") == 1) | ~(F.col("within_time_threshold") & (F.col("within_float_threshold"))))

    return df.drop("duplicate_id", "within_time_threshold", "within_float_threshold")


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


def give_date_interval_and_flag_if_outside(
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
        By default will be a string called 'hours' if upper and lower intervals
        are input as days, define interval_format to 'days'. These are the only
        two possible formats.
    Notes
    -----
    Lower_interval should be a negative value if start_datetime_reference_column
    is after end_datetime_reference_column.
    """
    # by default, Hours but if days, apply change factor:
    if interval_format == "hours":  # to convert hours to seconds
        conversion_factor = 3600  # 1h has 60s*60min seconds = 3600 seconds
    elif interval_format == "days":
        conversion_factor = 86400  # 1 day has 60s*60min*24h seconds = 86400 seconds

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
