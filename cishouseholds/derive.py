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
    """Criteria - returns data with new column which is a substring
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

def mean_across_columns(df: DataFrame, new_column_name, column_names):
    columns = [F.col(name) for name in column_names]
    average_expression = sum(column for column in columns)/len(columns)
    return df.withColumn(new_column_name, average_expression)
