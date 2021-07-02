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
