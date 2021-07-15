from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def create_column_from_coalesce(df, new_column_name, *args):
    """
    Create new column with values from coalesced columns.
    From households_aggregate_processes.xlsx, derivation number 6.
    D6: V1, or V2 if V1 is missing

    Parameters
    ----------
    df: pyspark.sql.DataFrame
    new_column_name: string
    *args: string
        name of columns to coalesce

    Return
    ------
    df: pyspark.sql.DataFrame

    """
    return df.withColumn(colName=new_column_name, col=F.coalesce(*args))


def substring_column(df: DataFrame, new_column_name, column_to_substr, start_position, len_of_substr):
    """
    Criteria - returns data with new column which is a substring
    of an existing variable
    Parameters
    ----------
    df: pyspark.sql.DataFrame
    new_column_name: string
    column_to_substr: string
    start_position: integer
    len_of_substr: integer

    Return
    ------
    df: pyspark.sql.DataFrame

    """
    df = df.withColumn(new_column_name, F.substring(column_to_substr, start_position, len_of_substr))

    return df


def derive_ctpattern(df: DataFrame, column_names, spark_session):
    """
    Derive a new column containing string of pattern in
    ["N only", "OR only", "S only", "OR+N", "OR+S", "N+S", "OR+N+S", NULL]
    indicating which ct_* columns indicate a positive result.
    From households_aggregate_processes.xlsx, derivation number 7.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
    column_names: list of string
    spark_session: pyspark.sql.SparkSession

    Return
    ------
    df: pyspark.sql.DataFrame
    """
    assert len(column_names) == 3

    indicator_list = ["indicator_" + column_name for column_name in column_names]

    lookup_df = spark_session.createDataFrame(
        data=[
            (0, 0, 0, None),
            (1, 0, 0, "OR only"),
            (0, 1, 0, "N only"),
            (0, 0, 1, "S only"),
            (1, 1, 0, "OR+N"),
            (1, 0, 1, "OR+S"),
            (0, 1, 1, "N+S"),
            (1, 1, 1, "OR+N+S"),
        ],
        schema=indicator_list + ["ctpattern"],
    )

    for column_name in column_names:
        df = df.withColumn("indicator_" + column_name, F.when(F.col(column_name) > 0, 1).otherwise(0))

    df = df.join(F.broadcast(lookup_df), on=indicator_list, how="left").drop(*indicator_list)

    return df


def mean_across_columns(df: DataFrame, new_column_name: str, column_names: list):
    """
    Create a new column containing the mean of multiple existing columns.

    # Caveat:
    # 0 values are treated as nulls.

    Parameters
    ----------
    df
    new_column_name
        name of column to be created
    column_names
        list of column names to calculate mean across
    """
    columns = [F.col(name) for name in column_names]

    df = df.withColumn("temporary_column_count", F.lit(0))
    for column in column_names:
        df = df.withColumn(
            "temporary_column_count",
            F.when((F.col(column) > 0), F.col("temporary_column_count") + 1).otherwise(F.col("temporary_column_count")),
        )

    # Sum with NULL values removed
    average_expression = sum(F.coalesce(column, F.lit(0)) for column in columns) / F.col("temporary_column_count")
    df = df.withColumn(new_column_name, average_expression)
    df = df.drop("temporary_column_count")
    return df


def assign_column_uniform_value(df: DataFrame, column_name_to_assign: str, uniform_value):
    """
    Assign a column with a uniform value.
    From households_aggregate_processes.xlsx, derivation number 11.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    uniform_value
        Value to be set in column.

    Return
    ------
    pyspark.sql.DataFrame

    Notes
    -----
    uniform_value will work as int, float, bool, str, datetime -
            iterables/collections raise errors.
    """
    return df.withColumn(column_name_to_assign, F.lit(uniform_value))


def assign_column_convert_to_date(df: DataFrame, column_name_to_assign: str, reference_column: str):
    """
    Assign a column with a TimeStamp to a DateType
    From households_aggregate_processes.xlsx, derivation number 13.
    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    reference_column
        Column of TimeStamp type to be converted

    Returns
    -------
    pyspark.sql.DataFrame

    Notes
    -----
    Expects reference column to be a timestamp and therefore castable.

    """

    return df.withColumn(column_name_to_assign, F.to_date(F.col(reference_column)))


def assign_single_column_from_split(
    df: DataFrame, column_name_to_assign: str, reference_column: str, split_on: str = " ", item_number: int = 0
):
    """
    Assign a single column with the values from an item within a reference column that has been split.
    Can specify the split string and item number.

    Gets the first item after splitting on single space (" ") by default.

    Returns null when the specified item does not exist in the split.

    From households_aggregate_processes.xlsx, derivation number 14.
        Column of TimeStamp type to be converted

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    reference_column
        Column of TimeStamp type to be converted


    Returns
    -------
    pyspark.sql.DataFrame

    Notes
    -----
    Expects reference column to be a timestamp and therefore castable.

    """

    return df.withColumn(column_name_to_assign, F.split(F.col(reference_column), split_on).getItem(item_number))
