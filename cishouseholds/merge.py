from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cishouseholds.filter import assign_date_interval_and_flag


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


def one_to_many_bloods_flag(df: DataFrame, column_name_to_assign: str, group_by_column: str):
    df = assign_date_interval_and_flag(
        df, "out_of_date_range_blood", "diff_interval", "visit_date", "received_ox_date", -24, 48
    )
    df = assign_merge_process_group_flag(
        df,
        "identify_one_to_many_bloods_flag",
        "out_of_date_range_blood",
        "count_iqvia",
        "==1",
        "count_blood",
        ">1",
    )
    df = df.withColumn(
        "1_to_m_bloods_bool_flag",
        F.when((F.col("identify_one_to_many_bloods_flag") == 1), 0).otherwise(1),
    )
    window = Window.partitionBy(group_by_column).orderBy("1_to_m_bloods_bool_flag", "diff_interval_hours", "visit_date")
    df = df.withColumn("row_num", F.row_number().over(window))
    df = df.withColumn(
        "drop flag", F.when(((F.col(column_name_to_assign) == 1) & (F.col("row_num") != 1)), 1).otherwise(None)
    )
    df.show()
    return df.drop("out_of_date_range_blood", "identify_one_to_many_bloods_flag", "row_num", "1_to_m_bloods_bool_flag")
