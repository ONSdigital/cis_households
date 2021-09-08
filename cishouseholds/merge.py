from typing import Union

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


def assign_time_difference_and_flag_if_outside_interval(
    df: DataFrame,
    column_name_outside_interval_flag: str,
    column_name_time_difference: str,
    start_datetime_reference_column: str,
    end_datetime_reference_column: str,
    interval_lower_bound: Union[int, float],
    interval_upper_bound: Union[int, float],
    interval_bound_format: str = "hours",
) -> DataFrame:
    """
    Creates column to give the time difference in either hours (by default) or days
    between two columns, and creates associated column to flag whether the difference is
    between specified lower and upper bounds (inclusive). If the difference is outside
    of these bounds, return 1, otherwise None.

    Parameters
    ----------
    df
    column_name_outside_interval_flag
        Name of the column that returns whether the difference in datetimes is
        within the upper/lower bounds. If within, return None, otherwise
        an integer 1.
    column_name_time_difference
        Name of the column that returns the difference between start and end
        datetimes
    start_datetime_reference_column
        Reference column with datetime in string format yyyy-mm-dd hh:mm:ss.
    end_datetime_reference_column
        Reference column with datetime in string format yyyy-mm-dd hh:mm:ss.
    interval_lower_bound
        The minimum accepted time difference interval between
        end_datetime_reference_column and start_datetime_reference_column.
    interval_upper_bound
        The maximum accepted time difference interval between
        end_datetime_reference_column and start_datetime_reference_column
    interval_bound_format
        By default will be a string called 'hours'. If upper and lower interval
        bounds are input as days, define interval_format to 'days'.
        These are the only two possible formats.

    Notes
    -----
    Lower_interval should be a negative value if start_datetime_reference_column
    is after end_datetime_reference_column.
    """

    if interval_bound_format == "hours":
        conversion_factor = 3600  # 1h has 60s*60min seconds = 3600 seconds
    elif interval_bound_format == "days":
        conversion_factor = 86400  # 1 day has 60s*60min*24h seconds = 86400 seconds

    # FORMULA: (end_datetime_reference_column - start_datetime_reference_column) in
    # seconds/conversion_factor in seconds
    df = df.withColumn(
        column_name_time_difference,
        (
            F.to_timestamp(F.col(end_datetime_reference_column)).cast("long")
            - F.to_timestamp(F.col(start_datetime_reference_column)).cast("long")
        )
        / conversion_factor,
    )

    return df.withColumn(
        column_name_outside_interval_flag,
        F.when(~F.col(column_name_time_difference).between(interval_lower_bound, interval_upper_bound), 1).otherwise(
            None
        ),
    )


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


def assign_merge_process_group_flag(
    df: DataFrame,
    column_name_to_assign: str,
    out_of_date_range_flag: str,
    count_barcode_labs_column_name: str,
    count_barcode_labs_condition: str,
    count_barcode_voyager_column_name: str,
    count_barcode_voyager_condition: str,
):
    """
    Combine three conditions to create flag indicating record to be processed in forthcoming matching process.
    This is run for each of 1:1, 1:many, many:1 and many:many to identify the relevant processing group.

    Parameters
    ----------
    df
    column_name_to_assign
    out_of_date_range_flag
    count_barcode_labs_column_name
    count_barcode_labs_condition
    count_barcode_voyager_column_name
    count_barcode_voyager_condition
    """

    count_barcode_labs_condition = F.expr(f"{count_barcode_labs_column_name} {count_barcode_labs_condition}")
    count_barcode_voyager_condition = F.expr(f"{count_barcode_voyager_column_name} {count_barcode_voyager_condition}")

    df = df.withColumn("count_barcode_labs_flag", F.when(count_barcode_labs_condition, 1))
    df = df.withColumn("count_barcode_voyager_flag", F.when(count_barcode_voyager_condition, 1))

    return df.withColumn(
        column_name_to_assign,
        F.when(
            (
                F.col(out_of_date_range_flag).isNull()
                & (F.col("count_barcode_labs_flag") + F.col("count_barcode_voyager_flag") == 2)
            ),
            1,
        ).otherwise(None),
    ).drop("count_barcode_labs_flag", "count_barcode_voyager_flag")


def many_to_one_antibody_flag(df: DataFrame, column_name_to_assign: str, group_by_column: str):
    """
    Many (Voyager) to one (antibody) matching process. Creates a flag to identify rows which doesn't match
    required criteria (to be filtered later)

    Parameters
    ----------
    df
    column_name_to_assign

    """
    df = assign_merge_process_group_flag(
        df,
        "identify_many_to_one_antibody_flag",
        "out_of_date_range_antibody",
        "count_barcode_antibody",
        "==1",
        "count_barcode_voyager",
        ">1",
    )

    window = Window.partitionBy(group_by_column)

    df = df.withColumn(
        "antibody_barcode_cleaned_count",
        F.sum(F.when(F.col("identify_many_to_one_antibody_flag") == 1, 1).otherwise(None)).over(window),
    )

    df = df.withColumn(column_name_to_assign, F.when(F.col("antibody_barcode_cleaned_count") > 1, 1).otherwise(None))

    return df.drop("antibody_barcode_cleaned_count", "identify_many_to_one_antibody_flag")


def many_to_many_flag(
    df: DataFrame,
    drop_flag_column_name_to_assign: str,
    group_by_column: str,
    ordering_columns: str,
    process_type: str,
    failed_flag_column_name_to_assign: str,
):
    """
    Many (Voyager) to Many (antibody) matching process.
    Creates flag for records to be dropped as non-optimal matches, and separate flag for records where process fails.

    Parameters
    ----------
    df
    drop_flag_column_name_to_assign
        Name of column to indicate record is to be dropped
    group_by_column
        Names of columns to group dataframe
    ordering_columns
        Names of columns to order each group
    process_type
        Defines which many-to-many matching process is being carried out.
        Must be 'antibody' or 'swab'
    failed_flag_column_name_to_assign
        Name of column to indicate record has failed validation logic
    """

    df = assign_merge_process_group_flag(
        df,
        "identify_many_to_many_flag",
        "out_of_date_range_" + process_type,
        "count_barcode_" + process_type,
        ">1",
        "count_barcode_voyager",
        ">1",
    )

    # Need to reapply ordering
    window = Window.partitionBy(group_by_column, "identify_many_to_many_flag")

    if process_type == "antibody":
        column_to_validate = "antibody_test_result_classification"
    elif process_type == "swab":
        column_to_validate = "pcr_result_classification"
    else:
        print("Error")

    df = df.withColumn(
        "classification_different_to_first",
        F.sum(F.when(F.col(column_to_validate) == F.first(column_to_validate).over(window), None).otherwise(1)).over(
            window
        ),
    )

    df = df.withColumn(
        failed_flag_column_name_to_assign,
        F.when(
            F.last("classification_different_to_first").over(window).isNotNull()
            & (F.col("identify_many_to_many_flag") == 1),
            1,
        ).otherwise(None),
    )

    df = df.withColumn("record_processed", F.lit(None))
    unique_id_lab_str = "unique_id_" + process_type

    while df.filter(df.record_processed.isNull()).count() > 0:
        window = Window.partitionBy(group_by_column, "identify_many_to_many_flag", "record_processed").orderBy(
            *ordering_columns
        )
        df = df.withColumn("row_number", F.row_number().over(window))
        df = df.withColumn(
            "record_processed",
            F.when(
                (
                    (F.col(unique_id_lab_str) == (F.first(unique_id_lab_str).over(window)))
                    | (F.col("unique_id_voyager") == (F.first("unique_id_voyager").over(window)))
                )
                & (F.col("row_number") != 1),
                1,
            ).otherwise(F.col("record_processed")),
        )

        df = df.withColumn(drop_flag_column_name_to_assign, F.when(F.col("record_processed") == 1, 1).otherwise(None))

        df = df.withColumn(
            "record_processed",
            F.when((F.col("row_number") == 1) & (F.col(drop_flag_column_name_to_assign).isNull()), 0).otherwise(
                F.col("record_processed")
            ),
        )

    return df.drop("identify_many_to_many_flag", "classification_different_to_first", "record_processed", "row_number")
